package eagle

const (
	maxFileSizeDefault          = 1024 * 1024 * 64
	maxTombstoneFileSizeDefault = 1024 * 1024
	compactionThresholdDefault  = 0.75
)

type Options struct {
	MaxFileSize             uint32
	MaxTombstoneFileSize    uint32
	RootDir                 string
	FileCompactionThreshold float64
	EncryptKey              []byte
}

func (opts *Options) UseEncryption() bool {
	return opts.EncryptKey != nil
}

// DefaultOptions returns a list of recommended options for good performance.
func DefaultOptions(rootDir string) Options {
	return Options{
		RootDir:                 rootDir,
		MaxFileSize:             maxFileSizeDefault,
		MaxTombstoneFileSize:    maxTombstoneFileSizeDefault,
		FileCompactionThreshold: compactionThresholdDefault,
		EncryptKey:              nil,
	}
}

func (opts Options) WithMaxFileSize(size uint32) Options {
	opts.MaxFileSize = size
	return opts
}

// WithMaxStaleDataThreshold represents the threshold of stale data above which a bucket is rewritten from scratch.
// In particular, for a given file f, if staleSize(f) / size(f) > threshold and size(f) > minRewriteFileSize, then
// a bucket is entirely rewritten, and the old file is discarded.
func (opts Options) WithFileCompactionThreshold(th float64) Options {
	opts.FileCompactionThreshold = th
	return opts
}

// WithEncryptionKey sets the encryption key which will be used for crypting and decrypting each frame.
func (opts Options) WithEncryptionKey(key []byte) Options {
	opts.EncryptKey = key
	return opts
}
