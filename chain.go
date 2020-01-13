package cron

// Job represent one method for calling from remote.
type Job func()

// Middleware deal with input Job and output Job
type Middleware func(Job) Job

// Chain connect middleware into one middleware.
func Chain(chains ...Middleware) Middleware {
	return func(next Job) Job {
		for i := len(chains) - 1; i >= 0; i-- {
			next = chains[i](next)
		}
		return next
	}
}
