package store

import (
	"strings"
)

// Supports unix/ruby-style glob patterns:
//  - `*` matches zero or more chars in a single path component
//  - `**` matches zero or more chars in zero or more components
func translateGlob(pattern string) (regexp string) {
	outs := make([]string, len(pattern))
	i, double := 0, false
	for _, c := range pattern {
		switch c {
		default:
			outs[i] = string(c)
			double = false
		case '.':
			outs[i] = `\.`
			double = false
		case '*':
			if double {
				outs[i-1] = `.*`
			} else {
				outs[i] = `[^/]*`
			}
			double = !double
		}
		i++
	}
	outs = outs[0:i]

	return "^" + strings.Join(outs, "") + "$"
}
