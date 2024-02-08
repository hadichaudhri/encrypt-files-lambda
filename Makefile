deploy: build
	cargo lambda deploy encrypt-files
watch:
	cargo lambda watch
build:
	cargo lambda build --release
invoke:
	cargo lambda invoke encrypt-files
