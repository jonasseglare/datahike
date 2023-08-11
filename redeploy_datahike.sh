#!/bin/bash
rm -rf /home/jonas/.m2/repository/io/replikativ/datahike/0.6.1544
bb pom
bb jar
bb install
