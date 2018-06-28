FILE = paper

all:
	pdflatex $(FILE)
	bibtex $(FILE)
	pdflatex $(FILE)
	pdflatex $(FILE)
	rm -f paper.thm paper.out paper.aux paper.bbl paper.blg paper.log  paper.ps
	mv paper.pdf minicrypt2.pdf
	open minicrypt2.pdf

fast:
	latex $(FILE)
	dvips $(FILE).dvi
	ps2pdf $(FILE).ps
	rm -f paper.aux paper.bbl paper.blg paper.log paper.dvi paper.ps

clean:
	rm -f paper.aux paper.bbl paper.blg paper.log paper.pdf paper.dvi paper.ps paper.out paper.thm tex/*.log plots/*-eps-converted-to.pdf

view: all
	evince $(FILE).pdf

SPELLTEX=$(shell egrep '^\\\input' $(FILE).tex | sed 's/^.*input[^{]*{//' | sed 's/}.*$$/.tex/')
spell:
	for i in $(SPELLTEX); do \
	  echo $$i; \
	  aspell -c --home-dir=. $$i; \
	done
