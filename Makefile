.PHONY: all cli-linux cli-darwin cli-windows format lint test clean

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

version = `cat VERSION`

src_dir       = src
res_dir       = resources
build_dir     = build

depend_log    = $(build_dir)/.depend
build_log     = $(build_dir)/.build
merge_log     = $(build_dir)/.merge

tools_dir     = $(build_dir)/tools
codegen_link  = "https://raw.githubusercontent.com/elodina/go-avro/master/codegen/codegen.go"
codegen       = $(tools_dir)/codegen.go

avro_dir      = avro
cluster_avsc  = $(avro_dir)/cluster.avsc
playbook_avsc = $(avro_dir)/playbook.avsc

coverage_dir  = $(build_dir)/coverage
coverage_out  = $(coverage_dir)/coverage.out
coverage_html = $(coverage_dir)/coverage.html

generated_dir    = $(build_dir)/generated
generated_schema = $(generated_dir)/schema_generated.go
generated_data   = $(generated_dir)/data_generated.go

merge_src_dir = $(build_dir)/src
output_dir    = $(build_dir)/bin
release_dir   = $(build_dir)/release

linux_dir     = $(output_dir)/linux
darwin_dir    = $(output_dir)/darwin
windows_dir   = $(output_dir)/windows

bin_name      = dataflow-runner
bin_linux     = $(linux_dir)/$(bin_name)
bin_darwin    = $(darwin_dir)/$(bin_name)
bin_windows   = $(windows_dir)/$(bin_name)

gox           = "github.com/mitchellh/gox"

# -----------------------------------------------------------------------------
#  BUILDING
# -----------------------------------------------------------------------------

all: $(merge_log) $(build_log)

$(merge_log): $(depend_log)
	mkdir -p $(output_dir)
	rm -rf $(merge_src_dir)
	mkdir -p $(merge_src_dir)

	cp $(shell find $(generated_dir) -maxdepth 5 -name "*.go") $(merge_src_dir)
	cp $(filter-out $(go_test_files), $(shell find $(src_dir) -maxdepth 5 -name "*.go")) $(merge_src_dir)
	cp $(shell find $(src_dir) -maxdepth 5 -name "*_test.go") $(merge_src_dir)

	GO111MODULE=on go get -t ./$(merge_src_dir)

	@echo Source merged at: `/bin/date "+%Y-%m-%d---%H-%M-%S"` >> $(merge_log);

$(build_log): cli-linux cli-darwin cli-windows
	@echo Build success at: `/bin/date "+%Y-%m-%d---%H-%M-%S"` >> $(build_log);

cli-linux: $(merge_log)
	GO111MODULE=on go run $(gox) -osarch=linux/amd64 -output=$(bin_linux) ./$(merge_src_dir)
	zip -rj $(output_dir)/dataflow_runner_$(version)_linux_amd64.zip $(bin_linux)

cli-darwin: $(merge_log)
	GO111MODULE=on go run $(gox) -osarch=darwin/amd64 -output=$(bin_darwin) ./$(merge_src_dir)
	zip -rj $(output_dir)/dataflow_runner_$(version)_darwin_amd64.zip $(bin_darwin)

cli-windows: $(merge_log)
	GO111MODULE=on go get github.com/konsorten/go-windows-terminal-sequences || true
	GO111MODULE=on go run $(gox) -osarch=windows/amd64 -output=$(bin_windows) ./$(merge_src_dir)
	zip -rj $(output_dir)/dataflow_runner_$(version)_windows_amd64.zip $(bin_windows).exe

# -----------------------------------------------------------------------------
#  FORMATTING
# -----------------------------------------------------------------------------

format:
	GO111MODULE=on go fmt ./$(src_dir)
	gofmt -s -w ./$(src_dir)

lint:
	GO111MODULE=on go get -u golang.org/x/lint/golint
	golint ./$(src_dir)

# -----------------------------------------------------------------------------
#  TESTING
# -----------------------------------------------------------------------------

test: $(merge_log)
	mkdir -p $(coverage_dir)
	GO111MODULE=on go test -parallel=1 ./$(merge_src_dir) -tags test -v -covermode=count -coverprofile=$(coverage_out)

	grep -v 'data_generated.go\|schema_generated.go' $(coverage_out) > $(coverage_out)2
	mv $(coverage_out)2 $(coverage_out)
	sed -i 's/github.com\/snowplow\/dataflow-runner\/build/github.com\/snowplow\/dataflow-runner/g' $(coverage_out)

	GO111MODULE=on go tool cover -html=$(coverage_out) -o $(coverage_html)

# -----------------------------------------------------------------------------
#  CLEANUP
# -----------------------------------------------------------------------------

clean:
	rm -rf $(build_dir)

# -----------------------------------------------------------------------------
#  DEPENDENCIES
# -----------------------------------------------------------------------------

$(depend_log):
	rm -f $(depend_log)
	rm -rf $(generated_dir)
	mkdir -p $(generated_dir)
	mkdir -p $(tools_dir)

	wget -N $(codegen_link) -O $(codegen)

	GO111MODULE=on go run $(codegen) --schema $(cluster_avsc) --schema $(playbook_avsc) --out $(generated_schema)
	GO111MODULE=on go run github.com/go-bindata/go-bindata/go-bindata -o $(generated_data) $(avro_dir)

	@echo Dependencies generated at: `/bin/date "+%Y-%m-%d---%H-%M-%S"` >> $(depend_log);
