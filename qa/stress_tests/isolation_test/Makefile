# PostgreSQL Database Management System
# (formerly known as Postgres, then as Postgres95)
# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
# 
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose, without fee, and without a written agreement
# is hereby granted, provided that the above copyright notice and this
# paragraph and the following two paragraphs appear in all copies.
# 
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
# LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
# DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
# 
# THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
# AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
# ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
# PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#
# Makefile for isolation tests
#

FLEX = /usr/bin/flex
FLEXFLAGS =  $(LFLAGS)
BISON = /usr/bin/bison
BISONFLAGS =  -Wno-deprecated -d 

# keep intrim files
.SECONDARY:

# to generate .c file from lex file
%.c: %.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o '$@' $<
else
	@$(missing) flex $< '$@'
endif

# to generate .c and .h files from yacc file
%.c: %.y
	$(if $(BISON_CHECK_CMD),$(BISON_CHECK_CMD))
ifdef BISON
	$(BISON) $(BISONFLAGS) -o $@ $<
else
	@$(missing) bison $< $@
endif

# to compile the .c files that repy on PG headers.
# valid for Ubuntu, might need to expand for other platforms (TODO)
libpq_headers = /usr/include/postgresql/

OBJS = \
	$(WIN32RES) \
	isolationtester.o \
	specparse.o \
	specscanner.o

# compile options, we force -g for debug convinience
CFLAGS := -I. -I$(libpq_headers) -g \
		-Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement \
        -Werror=vla -Wendif-labels -Wmissing-format-attribute -Wimplicit-fallthrough=3 \
		-Wcast-function-type -Wshadow=compatible-local -Wformat-security -fno-strict-aliasing \
		-fwrapv -fexcess-precision=standard -Wno-format-truncation -Wno-stringop-truncation 

LDFLAGS =

# valid mostly for Ubuntu, need to extend for other platforms (TODO)
LIBS = -L/usr/lib/x86_64-linux-gnu/ -lpq

all: isolationtester

isolationtester: $(OBJS) 
	$(CC) $(CFLAGS) $^  $(LIBS) -o $@

specparse.c: specparse.y

# See notes in src/backend/parser/Makefile about the following two rules
specparse.h: specparse.c
	touch $@

# Force these dependencies to be known even without dependency info built:
specparse.o specscanner.o: specparse.h Makefile

clean:
	rm -f isolationtester $(OBJS) specparse.c specparse.h specscanner.c
	rm -rf output

# default connect properties, presumably always same for dev and qa env
TEST_DB ?= $(TESTDB)
ifeq ("$(TEST_DB)","")
	TEST_DB = defaultdb
endif
# $(info $(TEST_DB))

TEST_USER ?= $(TESTUSER)
ifeq ("$(TEST_USER)","")
	TEST_USER = root
endif
# $(info $(TEST_USER))

TEST_PW ?= $(TESTPW)
ifeq ("$(TEST_PW)","")
	TEST_PW = 123456
endif
# $(info $(TEST_PW))

TEST_PORT ?= $(TESTPORT)
ifeq ("$(TEST_PORT)","")
	TEST_PORT = $(DFLT_KW_PORT)
	ifeq ("$(TEST_PORT)","")
		TEST_PORT = 26257
	endif
endif
# $(info $(TEST_PORT))

# determine target IP to test:
# 1) check if 127.0.0.1:$(TESTPORT) is being listened, and if it's not, goto 2)
# 2) do best to find out a valid IP address other than loopback on this host
TEST_IP ?= $(TESTIP)
ifeq ("$(TEST_IP)","")
	TEST_IP = $(LOCAL_HOST_IP)
	ifeq ("$(TEST_IP)","")
		RET := $(shell nc -z 127.0.0.1 $(TEST_PORT) && echo 0 )
		ifeq ("$(RET)", "0")
			TEST_IP = 127.0.0.1
		else
			TEST_IP = $(shell ip -o -4 a | grep -v -w 'lo' |grep -v docker | grep -v br- | awk '{print $$4}'| awk -F/ '{print $$1}' |tail -n 1 )
		endif
	endif
endif
# $(info $(TEST_IP))

#check serviceability
service_check:
	@echo "Performing serviceability check again $(TEST_IP):$(TEST_PORT) ..."
	@ret=`nc -z $(TEST_IP) $(TEST_PORT) ` ; \
	if [ $$? -ne 0 ]; then \
		echo "Target service $(TEST_IP):$(TEST_PORT) is NOT active.  Use env var TESTIP and TESTPORT to override target sevice"; \
		exit 1 ; \
	fi
	
# enumerate all the spec files under specs directory
SPECS := $(wildcard specs/*/*.spec)

# when make test is invoked:
# for each spec file under specs/ directory, we get it's relpath under specs, name it as "casefile"
# and then generate an output file name with suffix ".out" (outfile in below script),
# then run the test spec as input to isolationtester,
# the output is redirected to output/$outfile, and then compare the outfile with exp file under expected/
# file
test: $(SPECS) all service_check
# test: $(SPECS) all
	@echo "========================================"
	@echo "The service under test is: postgresql://$(TEST_USER):$(TEST_PW)@$(TEST_IP):$(TEST_PORT)/$(TEST_DB)"
	@echo "========================================"
	@for file in $(SPECS); do \
		casefile=`echo $$file | sed -e 's/^specs\///g'`; \
		relpath=`dirname $$file | sed -e 's/^specs\///g'`; \
	    mkdir -p "output/$$relpath"; \
		outfile=`echo $$casefile | sed -e 's/\.spec$$/\.out/g'`; \
		echo "Running test for: $$casefile"; \
        attempt=0; \
        success=false; \
        while [ $$attempt -lt 5 ]; do \
        	echo "Attempt $$(($$attempt + 1)) for $$casefile"; \
        	./isolationtester "postgresql://$(TEST_USER):$(TEST_PW)@$(TEST_IP):$(TEST_PORT)/$(TEST_DB)" < specs/$$casefile > output/$$outfile; \
        	if diff -q expected/$$outfile output/$$outfile > /dev/null; then \
        		echo "Test passed for $$casefile"; \
        		success=true; \
        		break; \
        	else \
        		echo "Test failed for $$casefile. Retrying..."; \
        	fi; \
        	attempt=$$(($$attempt + 1)); \
        done; \
        if [ "$$success" = false ]; then \
        	echo "Test failed after 5 attempts for $$casefile"; \
        	diff expected/$$outfile output/$$outfile; \
        	exit 1; \
        fi; \
	done
