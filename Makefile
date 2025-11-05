

.PHONY: build-dev  ## Build the development version of the package
build-dev:
	maturin develop

.PHONY: build-release  ## Build the release version of the package
build-prd:
	maturin develop --release

httpd:
	RUST_LOG=error ihttpd init