########################################################################
# National Liveability Dockerfile
#  - Modified by Carl Higgs 20190508
#  - Based on OSMNX Dockerfile, by Geoff Boeing
#  - see original OSMnx licence
#        - License: MIT, see full license in LICENSE.txt
#        - Web: https://github.com/gboeing/osmnx
#
# Build an image from the dockerfile:
# >>> docker build -t ind_li .
#
# Run bash in this container and export final conda environment to a yml file:
# >>> docker run --rm -it -u 0 --name ind_li -v %cd%:/home/jovyan/work ind_li /bin/bash
# >>> conda env export -n base > /home/jovyan/work/environment.yml
#
# Run jupyter lab in this container:
# >>> docker run --rm -it --name ind_li -p 8888:8888 -v %cd%:/home/jovyan/work ind_li
#
# Stop/delete all local docker containers/images:
# >>> docker stop $(docker ps -aq)
# >>> docker rm $(docker ps -aq)
# >>> docker rmi $(docker images -q)
########################################################################

FROM jupyter/base-notebook
LABEL maintainer="Carl Higgs <carl.higgs@rmit.edu.au>"

# symlink and permissions
USER root
RUN ln -s /opt/conda/bin/jupyter /usr/local/bin


# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

# Install OSM tools
RUN wget https://bretth.dev.openstreetmap.org/osmosis-build/osmosis-latest.tgz  \
 && mkdir osmosis                                                               \
 && mv osmosis-latest.tgz osmosis                                               \
 && cd osmosis                                                                  \
 && tar xvfz osmosis-latest.tgz                                                 \
 && rm osmosis-latest.tgz                                                       \
 && chmod a+x bin/osmosis                    

ENV TZ 'Australia/Melbourne'
RUN echo $TZ > /etc/timezone && \
apt-get update && apt-get install -y tzdata && \
rm /etc/localtime && \
ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
dpkg-reconfigure -f noninteractive tzdata && \
apt-get clean 
RUN apt-get install -y osm2pgsql
 
# configure conda and install packages in one RUN to keep image tidy
USER $NB_UID
RUN conda config --set show_channel_urls true                                                        \
 && conda config --prepend channels conda-forge                                                      \
 && conda update --strict-channel-priority --yes -n base conda                                       \
 && conda install --strict-channel-priority --update-all --force-reinstall --yes osmnx python-igraph \
 && conda install -y psycopg2 geoalchemy2 rasterio xlrd seaborn bokeh openpyxl                       \
 && conda clean --yes --all                                                                          \
 && conda info --all                                                                                 \
 && conda list

# additional packages for mapping and rendering html maps to png
RUN conda install -y geckodriver selenium basemap proj4 pyproj \
 && conda clean --yes --all                       \
 && conda info --all                              \
 && conda list
 
USER root 
RUN apt-get update && apt-get install -y firefox               
RUN apt-get install -y fonts-unfonts-core    \
 && apt-get install -y fonts-unfonts-extra   \
 && apt-get install -y fonts-tlwg-loma       \
 && apt-get install -y fonts-tlwg-loma-otf   \
 && apt-get install -y fonts-thai-tlwg
 
# set locale
RUN locale-gen en_AU.UTF-8 
ENV LC_ALL=en_AU.UTF-8  
ENV LANG=en_AU.UTF-8   
ENV LANGUAGE=en_AU.UTF-8

# launch notebook in the local working directory that we mount
USER $NB_UID 
WORKDIR /home/jovyan/work

# set default command to launch when container is run
CMD ["jupyter", "lab", "--no-browser", "--NotebookApp.token=''", "--NotebookApp.password=''"]

# to test, import OSMnx and print its version
RUN python -c "import osmnx; print(osmnx.__version__)"
