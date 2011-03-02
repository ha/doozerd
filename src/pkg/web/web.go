package web

import (
	"doozer/store"
	"doozer/util"
	"http"
	"io"
	"json"
	"log"
	"net"
	"runtime"
	"strings"
	"template"
	"websocket"
)

var Store *store.Store
var ClusterName, evPrefix string

var (
	mainTpl  = template.MustParse(main_html, nil)
	statsTpl = template.MustParse(stats_html, nil)
)

type info struct {
	Path string
}

type stringHandler struct {
	contentType string
	body        string
}

func (sh stringHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.SetHeader("content-type", sh.contentType)
	io.WriteString(w, sh.body)
}

func Serve(listener net.Listener) {
	prefix := "/d/" + ClusterName
	evPrefix = "/events" + prefix

	http.Handle("/", http.RedirectHandler("/view/d/"+ClusterName+"/", 307))
	http.HandleFunc("/stats.html", statsHtml)
	http.HandleFunc("/view/", viewHtml)
	http.Handle("/main.js", stringHandler{"application/javascript", main_js})
	http.Handle("/main.css", stringHandler{"text/css", main_css})
	http.HandleFunc(evPrefix+"/", evServer)

	http.Serve(listener, nil)
}

func send(ws *websocket.Conn, path string, evs <-chan store.Event, logger *log.Logger) {
	l := len(path) - 1
	for ev := range evs {
		ev.Getter = nil // don't marshal the entire snapshot
		ev.Path = ev.Path[l:]
		logger.Println("sending", ev)
		b, err := json.Marshal(ev)
		if err != nil {
			logger.Println(err)
			return
		}
		_, err = ws.Write(b)
		if err != nil {
			logger.Println(err)
			return
		}
	}
}

func evServer(w http.ResponseWriter, r *http.Request) {
	wevs := make(chan store.Event)
	logger := util.NewLogger(w.RemoteAddr())
	path := r.URL.Path[len(evPrefix):]
	logger.Println("new", path)

	glob, err := store.CompileGlob(path + "**")
	if err != nil {
		w.WriteHeader(400)
		return
	}

	wt := store.NewWatch(Store, glob)

	go func() {
		walk(path, Store, wevs)
		close(wevs)
	}()

	websocket.Handler(func(ws *websocket.Conn) {
		send(ws, path, wevs, logger)
		send(ws, path, wt.C, logger)
		wt.Stop()
		ws.Close()
	}).ServeHTTP(w, r)
}

func viewHtml(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/") {
		w.WriteHeader(404)
		return
	}
	var x info
	x.Path = r.URL.Path[len("/view"):]
	w.SetHeader("content-type", "text/html")
	mainTpl.Execute(w, x)
}

func statsHtml(w http.ResponseWriter, r *http.Request) {
	w.SetHeader("content-type", "text/html")
	statsTpl.Execute(w, runtime.MemStats)
}

func walk(path string, st *store.Store, ch chan store.Event) {
	for path != "/" && strings.HasSuffix(path, "/") {
		// TODO generalize and factor this into pkg store.
		path = path[0 : len(path)-1]
	}
	v, cas := st.Get(path)
	if cas != store.Dir {
		ch <- store.Event{0, path, v[0], cas, "", nil, nil}
		return
	}
	if path == "/" {
		path = ""
	}
	for _, ent := range v {
		walk(path+"/"+ent, st, ch)
	}
}
