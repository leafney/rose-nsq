package rnsq

type (
	Option func(opt *options)

	options struct {
		Secret string
	}
)

func SetSecret(secret string) Option {
	return func(opt *options) {
		opt.Secret = secret
	}
}
