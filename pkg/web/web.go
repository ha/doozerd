package web

import (
	"http"
	"io"
	"junta/store"
	"junta/util"
	"json"
	"log"
	"template"
	"websocket"
)

var Store *store.Store

var (
	mainTpl  = template.MustParse(main_html, nil)
	MainInfo = struct{ ClusterName string }{}
)

func init() {
	http.HandleFunc("/", mainHtml)
	http.HandleFunc("/main.js", mainJs)
	http.HandleFunc("/main.css", mainCss)
	http.Handle("/all", websocket.Handler(allServer))
}

func send(ws *websocket.Conn, evs chan store.Event, logger *log.Logger) {
	for ev := range evs {
		logger.Log("sending", ev)
		b, err := json.Marshal(ev)
		if err != nil {
			logger.Log(err)
			return
		}
		ws.Write(b)
	}
}

func allServer(ws *websocket.Conn) {
	evs, wevs := make(chan store.Event), make(chan store.Event)

	logger := util.NewLogger(ws.RemoteAddr().String())
	logger.Log("new")

	Store.Watch("**", evs)

	// TODO convert store.Snapshot to json and use that
	go walk("/", Store, wevs)

	send(ws, wevs, logger)
	send(ws, evs, logger)
}

func mainHtml(c *http.Conn, r *http.Request) {
	if r.URL.Path != "/" {
		c.WriteHeader(404)
		return
	}
	c.SetHeader("content-type", "text/html")
	mainTpl.Execute(MainInfo, c)
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
	if path == "" {
		close(ch)
	}
}
