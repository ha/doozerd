package web

import (
	"doozer/store"
	"http"
	"io"
	"json"
	"log"
	"net"
	"runtime"
	"strings"
	"old/template"
	"websocket"
)

var Store *store.Store
var ClusterName string

var (
	mainTpl  = template.MustParse(main_html, nil)
	statsTpl = template.MustParse(stats_html, nil)
)

type info struct {
	Name string
	Path string
}

type stringHandler struct {
	contentType string
	body        string
}

func (sh stringHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", sh.contentType)
	io.WriteString(w, sh.body)
}

func Serve(listener net.Listener) {
	http.HandleFunc("/", viewHtml)
	http.HandleFunc("/$stats.html", statsHtml)
	http.Handle("/$main.js", stringHandler{"application/javascript", main_js})
	http.Handle("/$main.css", stringHandler{"text/css", main_css})
	http.HandleFunc("/$events/", evServer)

	http.Serve(listener, nil)
}

func send(ws *websocket.Conn, path string, evs <-chan store.Event) {
	l := len(path) - 1
	for ev := range evs {
		ev.Getter = nil // don't marshal the entire snapshot
		ev.Path = ev.Path[l:]
		b, err := json.Marshal(ev)
		if err != nil {
			log.Println(err)
			return
		}
		_, err = ws.Write(b)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func evServer(w http.ResponseWriter, r *http.Request) {
	wevs := make(chan store.Event)
	path := r.URL.Path[len("/$events"):]

	glob, err := store.CompileGlob(path + "**")
	if err != nil {
		w.WriteHeader(400)
		return
	}

	rev, _ := Store.Snap()

	go func() {
		walk(path, Store, wevs)
		for {
			ch, err := Store.Wait(glob, rev+1)
			if err != nil {
				break
			}
			ev, ok := <-ch
			if !ok {
				break
			}
			wevs <- ev
			rev = ev.Rev
		}
		close(wevs)
	}()

	websocket.Handler(func(ws *websocket.Conn) {
		send(ws, path, wevs)
		ws.Close()
	}).ServeHTTP(w, r)
}

func viewHtml(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/") {
		w.WriteHeader(404)
		return
	}
	var x info
	x.Name = ClusterName
	x.Path = r.URL.Path
	w.Header().Set("content-type", "text/html")
	mainTpl.Execute(w, x)
}

func statsHtml(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "text/html")
	statsTpl.Execute(w, runtime.MemStats)
}

func walk(path string, st *store.Store, ch chan store.Event) {
	for path != "/" && strings.HasSuffix(path, "/") {
		// TODO generalize and factor this into pkg store.
		path = path[0 : len(path)-1]
	}
	v, rev := st.Get(path)
	if rev != store.Dir {
		ch <- store.Event{0, path, v[0], rev, "", nil, nil}
		return
	}
	if path == "/" {
		path = ""
	}
	for _, ent := range v {
		walk(path+"/"+ent, st, ch)
	}
}
