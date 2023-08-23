#!/bin/bash

docker run -it --entrypoint /bin/bash -v "$(pwd)/../data/":/data/ daily-update-microservice
# docker run -it --entrypoint /bin/bash -v /Users/mattman/src/canada-climate-data-exploration/data/:/data/ daily-update-microservice
