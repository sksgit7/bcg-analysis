build:
	rm -rf dist
	mkdir dist
	cp main.py ./dist
	cp config.json ./dist
	unzip Data.zip -d ./dist/
	cd ./src && zip -r ../dist/src.zip .
