all:	compile run upload
compile:
	javac PageRank.java

run:
	java PageRank

upload:
	git add .
	git commit -a -m update
	git push
