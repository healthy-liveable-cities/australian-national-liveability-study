cd..
docker run --rm -it -u jovyan --name ind_li --shm-size 2g --net="host" -v %cd%:/home/jovyan/work ind_li /bin/bash 
