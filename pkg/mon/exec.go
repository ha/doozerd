package mon

import (
    "os"
)

func exitedCleanly(w *os.Waitmsg) bool {
	return w.Exited() && w.ExitStatus() == 0
}
