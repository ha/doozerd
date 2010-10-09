package web

import (
	"http"
	"io"
	"junta/store"
	"junta/util"
	"json"
	"log"
	"net"
	"strings"
	"template"
	"websocket"
)

var Store *store.Store
var ClusterName, evPrefix string
var mainTpl  = template.MustParse(main_html, nil)

type info struct {
	Path string
}

func Serve(listener net.Listener) {
	prefix := "/j/" + ClusterName
	evPrefix = "/events" + prefix

	http.Handle("/", http.RedirectHandler("/view/j/"+ClusterName+"/", 307))
	http.HandleFunc("/view/", viewHtml)
	http.HandleFunc("/main.js", mainJs)
	http.HandleFunc("/main.css", mainCss)
	http.HandleFunc(evPrefix+"/", evServer)

	http.Serve(listener, nil)
}

func send(ws *websocket.Conn, path string, evs chan store.Event, logger *log.Logger) {
	defer close(evs)
	l := len(path) - 1
	for ev := range evs {
		ev.Getter = nil // don't marshal the entire snapshot
		ev.Path = ev.Path[l:]
		logger.Log("sending", ev)
		b, err := json.Marshal(ev)
		if err != nil {
			logger.Log(err)
			return
		}
		_, err = ws.Write(b)
		if err != nil {
			logger.Log(err)
			return
		}
	}
}

func evServer(c *http.Conn, r *http.Request) {
	evs, wevs := make(chan store.Event), make(chan store.Event)
	logger := util.NewLogger(c.RemoteAddr)
	path := r.URL.Path[len(evPrefix):]
	logger.Log("new", path)

	Store.Watch(path+"**", evs)

	// TODO convert store.Snapshot to json and use that
	go func() {
		walk(path, Store, wevs)
		close(wevs)
	}()

	websocket.Handler(func(ws *websocket.Conn) {
		send(ws, path, wevs, logger)
		send(ws, path, evs, logger)
		ws.Close()
	}).ServeHTTP(c, r)
}

func viewHtml(c *http.Conn, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/") {
		c.WriteHeader(404)
		return
	}
	var x info
	x.Path = r.URL.Path[len("/view"):]
	c.SetHeader("content-type", "text/html")
	mainTpl.Execute(x, c)
}

func mainJs(c *http.Conn, r *http.Request) {
	c.SetHeader("content-type", "application/javascript")
	io.WriteString(c, main_js)
}

func mainCss(c *http.Conn, r *http.Request) {
	c.SetHeader("content-type", "text/css")
	io.WriteString(c, main_css)
}

func walk(path string, st *store.Store, ch chan store.Event) {
	for path != "/" && strings.HasSuffix(path, "/") {
		// TODO generalize and factor this into pkg store.
		path = path[0:len(path)-1]
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
