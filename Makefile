#
# Makefile for Asterisk kafka resource
# Copyright (C) 2026, VSGroup
#
# This program is free software, distributed under the terms of
# the GNU General Public License Version 2. See the LICENSE file
# at the top of the source tree.
#

ASTLIBDIR:=$(shell awk '/moddir/{print $$3}' /etc/asterisk/asterisk.conf 2> /dev/null)
ifeq ($(strip $(ASTLIBDIR)),)
	MODULES_DIR:=$(INSTALL_PREFIX)/usr/lib/asterisk/modules
else
	MODULES_DIR:=$(INSTALL_PREFIX)$(ASTLIBDIR)
endif
ifeq ($(strip $(DOCDIR)),)
	DOCUMENTATION_DIR:=$(INSTALL_PREFIX)/var/lib/asterisk/documentation/thirdparty
else
	DOCUMENTATION_DIR:=$(INSTALL_PREFIX)$(DOCDIR)
endif
INSTALL = install
ASTETCDIR = $(INSTALL_PREFIX)/etc/asterisk
SAMPLENAME = kafka.conf.sample
CONFNAME = $(basename $(SAMPLENAME))

TARGET = res_kafka.so
OBJECTS = res_kafka.o kafka/cli.o kafka/config.o
CFLAGS += -I.
CFLAGS += -DHAVE_STDINT_H=1
CFLAGS += -Wall -Wextra -Wno-unused-parameter -Wstrict-prototypes -Wmissing-prototypes -Wmissing-declarations -Winit-self -Wmissing-format-attribute \
          -Wformat=2 -g -fPIC -D_GNU_SOURCE -D'AST_MODULE="res_kafka"' -D'AST_MODULE_SELF_SYM=__internal_res_kafka_self'
LIBS += -lrdkafka
LDFLAGS = -Wall -shared

.PHONY: install clean

$(TARGET): $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@ $(LIBS)

%.o: %.c $(HEADERS)
	$(CC) -c $(CFLAGS) -o $@ $<

install: $(TARGET)
	mkdir -p $(DESTDIR)$(MODULES_DIR)
	mkdir -p $(DESTDIR)$(DOCUMENTATION_DIR)
	install -m 644 $(TARGET) $(DESTDIR)$(MODULES_DIR)
	install -m 644 documentation/* $(DESTDIR)$(DOCUMENTATION_DIR)
	@echo " +----------- res_kafka installed ------------+"
	@echo " +                                            +"
	@echo " + res_kafka has successfully been installed   +"
	@echo " + If you would like to install the sample    +"
	@echo " + configuration file run:                    +"
	@echo " +                                            +"
	@echo " +              make samples                  +"
	@echo " +--------------------------------------------+"

clean:
	rm -f $(OBJECTS)
	rm -f $(TARGET)

samples:
	$(INSTALL) -m 644 $(SAMPLENAME) $(DESTDIR)$(ASTETCDIR)/$(CONFNAME)
	@echo " ------- res_kafka config installed ---------"
