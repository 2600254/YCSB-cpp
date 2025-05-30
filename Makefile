#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
#


#---------------------build config-------------------------

# Database bindings
BIND_ELASTICLSM ?= 0
BIND_BACHOPD ?= 0
BIND_WIREDTIGER ?= 0
BIND_LEVELDB ?= 0
BIND_ROCKSDB ?= 0
BIND_LMDB ?= 0
BIND_SQLITE ?= 0

# Extra options
DEBUG_BUILD ?=
EXTRA_CXXFLAGS ?=
EXTRA_LDFLAGS ?=

# HdrHistogram for tail latency report
BIND_HDRHISTOGRAM ?= 1
# Build and statically link library, submodule required
BUILD_HDRHISTOGRAM ?= 1

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O3 -march=native
	CPPFLAGS += -DNDEBUG
endif

ifeq ($(BIND_ELASTICLSM), 1)
	ifeq ($(ELASTIC_LSM_DIR),)
        $(error Please set the environment variable ELASTIC_LSM_DIR to the rootpath of elastic_lsm)
	endif
	ELASTIC_LSM_LIB_DIR ?= $(ELASTIC_LSM_DIR)
	ELASTIC_LSM_INCLUDE_DIR ?= $(ELASTIC_LSM_DIR)/include
	LDFLAGS += -L$(ELASTIC_LSM_LIB_DIR) -lrocksdb_elastic
	CXXFLAGS += -I$(ELASTIC_LSM_INCLUDE_DIR)
	SOURCES += $(wildcard elastic_lsm/*.cc)
endif

ifeq ($(BIND_BACHOPD), 1)
	ifeq ($(BACHOPD_DIR),)
        $(error Please set the environment variable BACHOPD_DIR to the rootpath of BACH-opd)
	endif
	BACHOPD_LIB_DIR ?= $(BACHOPD_DIR)/build \
					   $(BACHOPD_DIR)/build/include/folly
	BACHOPD_INCLUDE_DIR ?= $(BACHOPD_DIR)/include \
						   $(BACHOPD_DIR)/include/dynamic_bitset/include \
						   $(BACHOPD_DIR)/include/folly \
						   $(BACHOPD_DIR)/build/include/folly
	LDFLAGS += $(foreach dir,$(BACHOPD_LIB_DIR),-L$(dir)) -Wl,-rpath,$(BACHOPD_DIR)/build -lbach-opd -lfolly
	CXXFLAGS += $(foreach dir,$(BACHOPD_INCLUDE_DIR),-I$(dir))
	SOURCES += $(wildcard bach_opd/*.cc)
endif

ifeq ($(BIND_WIREDTIGER), 1)
	LDFLAGS += -lwiredtiger
	SOURCES += $(wildcard wiredtiger/*.cc)
endif

ifeq ($(BIND_LEVELDB), 1)
	LDFLAGS += -lleveldb
	SOURCES += $(wildcard leveldb/*.cc)
endif

ifeq ($(BIND_ROCKSDB), 1)
	LDFLAGS += -lrocksdb -ldl -lz -lsnappy -lzstd -lbz2 -llz4 -luring
	SOURCES += $(wildcard rocksdb/*.cc)
endif

ifeq ($(BIND_LMDB), 1)
	LDFLAGS += -llmdb
	SOURCES += $(wildcard lmdb/*.cc)
endif

ifeq ($(BIND_SQLITE), 1)
	LDFLAGS += -lsqlite3
	SOURCES += $(wildcard sqlite/*.cc)
endif

ifeq ($(BIND_BACHOPD), 1)
	CXXFLAGS += -std=c++20
else
	CXXFLAGS += -std=c++17
endif 
CXXFLAGS += -Wall -pthread $(EXTRA_CXXFLAGS) -I./
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc)
OBJECTS += $(SOURCES:.cc=.o)
DEPS += $(SOURCES:.cc=.d)
EXEC = ycsb

HDRHISTOGRAM_DIR = HdrHistogram_c
HDRHISTOGRAM_LIB = $(HDRHISTOGRAM_DIR)/src/libhdr_histogram_static.a

ifeq ($(BIND_HDRHISTOGRAM), 1)
ifeq ($(BUILD_HDRHISTOGRAM), 1)
	CXXFLAGS += -I$(HDRHISTOGRAM_DIR)/include
	OBJECTS += $(HDRHISTOGRAM_LIB)
else
	LDFLAGS += -lhdr_histogram
endif
CPPFLAGS += -DHDRMEASUREMENT
endif

all: $(EXEC)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

.cc.o:
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.d: %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cc=.o)' -o $@ $<

$(HDRHISTOGRAM_DIR)/CMakeLists.txt:
	@echo "Download HdrHistogram_c"
	@git submodule update --init

$(HDRHISTOGRAM_DIR)/Makefile: $(HDRHISTOGRAM_DIR)/CMakeLists.txt
	@cmake -DCMAKE_BUILD_TYPE=Release -S $(HDRHISTOGRAM_DIR) -B $(HDRHISTOGRAM_DIR)


$(HDRHISTOGRAM_LIB): $(HDRHISTOGRAM_DIR)/Makefile
	@echo "Build HdrHistogram_c"
	@make -C $(HDRHISTOGRAM_DIR)

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete
	$(RM) $(EXEC)

.PHONY: clean
