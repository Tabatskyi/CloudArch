package ring

import "math/bits"

const (
	c1 uint32 = 0xcc9e2d51
	c2 uint32 = 0x1b873593
	c3 uint32 = 0x85ebca6b
	c4 uint32 = 0xc2b2ae35
)

// Murmur3_32 implements the 32-bit MurmurHash3 algorithm.
func Murmur3_32(key []byte, seed uint32) uint32 {
	h := seed
	nblocks := len(key) / 4

	for i := 0; i < nblocks; i++ {
		k := uint32(key[i*4]) |
			uint32(key[i*4+1])<<8 |
			uint32(key[i*4+2])<<16 |
			uint32(key[i*4+3])<<24

		k *= c1
		k = bits.RotateLeft32(k, 15)
		k *= c2

		h ^= k
		h = bits.RotateLeft32(h, 13)
		h = h*5 + 0xe6546b64
	}

	tail := key[nblocks*4:]
	var k1 uint32
	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2
		h ^= k1
	}

	h ^= uint32(len(key))
	h ^= h >> 16
	h *= c3
	h ^= h >> 13
	h *= c4
	h ^= h >> 16

	return h
}
