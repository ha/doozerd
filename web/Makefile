TARG=web

GOFILES=\
	main.css.go\
	main.html.go\
	stats.html.go\
	main.js.go\
	web.go\

default: $(GOFILES)

%.css.go: %.css file2gostring
	./file2gostring $(TARG) $< < $< > $@.part
	mv $@.part $@

%.html.go: %.html file2gostring
	./file2gostring $(TARG) $< < $< > $@.part
	mv $@.part $@

%.js.go: %.js file2gostring
	./file2gostring $(TARG) $< < $< > $@.part
	mv $@.part $@

clean:
	rm *.css.go *.html.go *.js.go
