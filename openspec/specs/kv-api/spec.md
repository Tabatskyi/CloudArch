# kv-api Specification

## Purpose
Provides an OpenAPI 3.0 RESTful HTTP interface for registering table definitions and executing key-value record operations (`EXISTS`, `CREATE`, `READ`, `DELETE`) with compound primary key support.

## Requirements

### Requirement: Table Registration
The system SHALL provide an HTTP POST endpoint at `/api/v1/tables` allowing clients to define a table schema specifying a table name, a primary partition key, and an optional sort key.

#### Scenario: Register table with partition key only
- **WHEN** client posts a JSON schema `{"tableName": "users", "partitionKey": "user_id"}`
- **THEN** system registers the table and responds with HTTP status 201 Created

#### Scenario: Register table with compound primary key
- **WHEN** client posts a JSON schema `{"tableName": "events", "partitionKey": "user_id", "sortKey": "timestamp"}`
- **THEN** system registers the table and responds with HTTP status 201 Created

#### Scenario: Reject duplicate table registration
- **WHEN** client posts a table definition for a tableName that already exists
- **THEN** system rejects the request with HTTP status 409 Conflict

### Requirement: Record Creation
The system SHALL provide an HTTP PUT/POST endpoint at `/api/v1/tables/{table}/records` to insert or overwrite a record containing the primary key attributes and arbitrary JSON payload data.

#### Scenario: Create new record successfully
- **WHEN** client issues PUT to `/api/v1/tables/users/records` with partition key `user_123` and value payload
- **THEN** system persists the record and responds with HTTP status 200 or 201

#### Scenario: Reject record missing primary key attributes
- **WHEN** client attempts to insert a record missing the required partition key or sort key
- **THEN** system responds with HTTP status 400 Bad Request

### Requirement: Record Existence Check
The system SHALL provide an HTTP HEAD or GET check at `/api/v1/tables/{table}/records/{key}` to verify the existence of a record (`EXISTS` operation) without requiring the transmission of the full record payload.

#### Scenario: Existence check on existing record
- **WHEN** client checks existence of an active primary key in an existing table
- **THEN** system responds with HTTP status 200 OK indicating the key exists

#### Scenario: Existence check on non-existent record
- **WHEN** client checks existence of a key that does not exist in the table
- **THEN** system responds with HTTP status 404 Not Found

### Requirement: Record Retrieval
The system SHALL provide an HTTP GET endpoint at `/api/v1/tables/{table}/records/{key}` to retrieve the stored key-value payload for a given primary key.

#### Scenario: Read existing record
- **WHEN** client issues GET for a valid primary key that exists
- **THEN** system returns HTTP status 200 OK with the stored record JSON payload

#### Scenario: Read non-existent record
- **WHEN** client issues GET for a key that does not exist in the table
- **THEN** system returns HTTP status 404 Not Found with error details

### Requirement: Record Deletion
The system SHALL provide an HTTP DELETE endpoint at `/api/v1/tables/{table}/records/{key}` to remove a record associated with the specified primary key.

#### Scenario: Delete existing record
- **WHEN** client issues DELETE for an existing primary key
- **THEN** system deletes the record and responds with HTTP status 204 No Content or 200 OK

#### Scenario: Delete non-existent record idempotently
- **WHEN** client issues DELETE for a primary key that does not exist
- **THEN** system completes idempotently and responds with HTTP status 204 No Content

### Requirement: Intra-Partition Range Query
The system SHALL provide an HTTP GET endpoint at `/api/v1/tables/{table}/query` to retrieve records sharing a partition key, supporting optional sort key range boundaries.

#### Scenario: Query records by partition key and sort key range
- **WHEN** client issues GET to `/api/v1/tables/events/query?partitionKey=user_123&sortKeyFrom=1700000000&sortKeyTo=1700010000`
- **THEN** system returns HTTP status 200 OK with an ordered list of matching records

#### Scenario: Query partition key with no matching records
- **WHEN** client queries a valid partition key that contains no records within the requested sort key range
- **THEN** system returns HTTP status 200 OK with an empty list
