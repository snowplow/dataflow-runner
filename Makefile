.PHONY: all format lint test goveralls release release-dry clean

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

src_dir       = src
res_dir       = resources
build_dir     = build

depend_log    = $(build_dir)/.depend
build_log     = $(build_dir)/.build
merge_log     = $(build_dir)/.merge

cluster_avsc_link  = "http://iglucentral.com/schemas/com.snowplowanalytics.dataflowrunner/ClusterConfig/avro/1-1-0"
playbook_avsc_link = "http://iglucentral.com/schemas/com.snowplowanalytics.dataflowrunner/PlaybookConfig/avro/1-0-1"

tools_dir     = $(build_dir)/tools
codegen_link  = "https://raw.githubusercontent.com/elodina/go-avro/master/codegen/codegen.go"
codegen       = $(tools_dir)/codegen.go

avro_dir      = $(build_dir)/avro
cluster_avsc  = $(avro_dir)/cluster.avsc
playbook_avsc = $(avro_dir)/playbook.avsc

coverage_dir  = $(build_dir)/coverage
coverage_out  = $(coverage_dir)/coverage.out
coverage_html = $(coverage_dir)/coverage.html

generated_dir    = $(build_dir)/generated
generated_schema = $(generated_dir)/schema_generated.go
generated_data   = $(generated_dir)/data_generated.go

merge_src_dir = $(build_dir)/src
output_dir    = $(build_dir)/output

linux_dir     = $(output_dir)/linux
darwin_dir    = $(output_dir)/darwin
windows_dir   = $(output_dir)/windows

bin_name      = dataflow-runner
bin_linux     = $(linux_dir)/$(bin_name)
bin_darwin    = $(darwin_dir)/$(bin_name)
bin_windows   = $(windows_dir)/$(bin_name)

# -----------------------------------------------------------------------------
#  GOLANG FILES
# -----------------------------------------------------------------------------

go_gen_files  := $(shell find $(generated_dir) -maxdepth 5 -name "*.go")
go_test_files := $(shell find $(src_dir) -maxdepth 5 -name "*_test.go")
go_src_files  := $(filter-out $(go_test_files), $(shell find $(src_dir) -maxdepth 5 -name "*.go"))

# -----------------------------------------------------------------------------
#  BUILDING
# -----------------------------------------------------------------------------

all: $(merge_log) $(build_log)

$(merge_log): $(go_gen_files) $(go_src_files) $(go_test_files)
	mkdir -p $(output_dir)
	rm -rf $(merge_src_dir)
	mkdir -p $(merge_src_dir)

	cp $(go_gen_files) $(merge_src_dir)
	cp $(go_src_files) $(merge_src_dir)
	cp $(go_test_files) $(merge_src_dir)

	go get -u -t ./$(merge_src_dir)

	@echo Source merged at: `/bin/date "+%Y-%m-%d---%H-%M-%S"` >> $(merge_log);

$(build_log): $(merge_log)
	go get -u github.com/mitchellh/gox/...
	gox -osarch=linux/amd64 -output=$(bin_linux) ./$(merge_src_dir)
	gox -osarch=darwin/amd64 -output=$(bin_darwin) ./$(merge_src_dir)
	go get github.com/konsorten/go-windows-terminal-sequences || true
	gox -osarch=windows/amd64 -output=$(bin_windows) ./$(merge_src_dir)

	@echo Build success at: `/bin/date "+%Y-%m-%d---%H-%M-%S"` >> $(build_log);

# -----------------------------------------------------------------------------
#  FORMATTING
# -----------------------------------------------------------------------------

format:
	go fmt ./$(src_dir)
	gofmt -s -w ./$(src_dir)

lint:
	go get -u github.com/golang/lint/golint
	golint ./$(src_dir)

# -----------------------------------------------------------------------------
#  TESTING
# -----------------------------------------------------------------------------

test: $(merge_log)
	mkdir -p $(coverage_dir)
	go get -u golang.org/x/tools/cmd/cover/...
	go test ./$(merge_src_dir) -tags test -v -covermode=count -coverprofile=$(coverage_out)

	grep -v 'data_generated.go\|schema_generated.go' $(coverage_out) > $(coverage_out)2
	mv $(coverage_out)2 $(coverage_out)
	sed -i 's/github.com\/snowplow\/dataflow-runner\/build/github.com\/snowplow\/dataflow-runner/g' $(coverage_out)

	go tool cover -html=$(coverage_out) -o $(coverage_html)

goveralls: test
	go get -u github.com/mattn/goveralls/...
	goveralls -coverprofile=$(coverage_out) -service=travis-ci

# -----------------------------------------------------------------------------
#  RELEASE
# -----------------------------------------------------------------------------

release: all
	release-manager --config .release.yml --check-version --make-artifact --make-version --upload-artifact

release-dry: all
	release-manager --config .release.yml --check-version --make-artifact

# -----------------------------------------------------------------------------
#  CLEANUP
# -----------------------------------------------------------------------------

clean:
	rm -rf $(build_dir)

# -----------------------------------------------------------------------------
#  DEPENDENCIES
# -----------------------------------------------------------------------------

depend: $(depend_log)

$(cluster_avsc):
	mkdir -p $(avro_dir)
	wget -N $(cluster_avsc_link) -O $(cluster_avsc)
	sed -i 's/com.snowplowanalytics.dataflowrunner/com.snowplowanalytics.dataflowrunner.main/g' $(cluster_avsc)

$(playbook_avsc):
	mkdir -p $(avro_dir)
	wget -N $(playbook_avsc_link) -O $(playbook_avsc)
	sed -i 's/com.snowplowanalytics.dataflowrunner/com.snowplowanalytics.dataflowrunner.main/g' $(playbook_avsc)

$(depend_log): $(cluster_avsc) $(playbook_avsc)
	rm -f $(depend_log)
	rm -rf $(generated_dir)
	mkdir -p $(generated_dir)
	mkdir -p $(tools_dir)

	wget -N $(codegen_link) -O $(codegen)

	go get -u github.com/elodina/go-avro/...
	go run $(codegen) --schema $(cluster_avsc) --schema $(playbook_avsc) --out $(generated_schema)

	go get -u github.com/go-bindata/go-bindata/...
	go-bindata -o $(generated_data) $(avro_dir)

	@echo Dependencies generated at: `/bin/date "+%Y-%m-%d---%H-%M-%S"` >> $(depend_log);

include $(depend_log)
