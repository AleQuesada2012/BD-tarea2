## Big Data en el Programa para Ciencia de los Datos, FUNDATEC
### Tarea 1: Alejandro Quesada, Joshua Solís
El repositorio actual comprende los archivos utilizados durante el trabajo en la tarea.

Debe ejecutarse en este orden:
1. `bash build.sh` para crear la imagen con Docker.
2. `bash run-container.sh`. Esto levanta un contenedor usando la imagen del paso anterior, con el nombre `bd-tarea1`
3. Asegurarse de estar desde el shell de Bash dentro de la imagen.
4. `bash run-program.sh` se encarga de ejecutar el `spark-submit ...` y las pruebas unitarias en un solo script

### Ejecución separada de las pruebas unitarias (pytest)
Para las pruebas unitarias, aunque el script `run-program.sh` contiene la instrucción necesaria, se pueden ejecutar por separado utilizando el comando: `pytest`.

### Datos utilizados para el programa principal
En el programa principal, tenemos un `import` peculiar pues es un módulo auxiliar nuestro llamado `data_init.py`.
Este módulo se encarga de crear la información en los 3 archivos `.csv` necesarios para ejecutar el programa principal, ya que no sabíamos cuánto era suficiente entonces pensamos en crear, de manera aleatoria, 100 atletas para el `atleta.csv` y generar 50 registros aleatorios para cada actividad (i.e. nadar y correr) respectivamente.

Es por esto que el archivo `data_init.py` se ejecuta de manera automáticamente al correr el programa, asegurándose que el programa principal tenga la información necesaria. También, esto influyó en nuestras pruebas unitarias especialmente en el campo de unir los datos inicialmente. Esto se puede ver más en profundidad al **revisar el código del archivo**: `tests-join.py`.


### Otras notas importantes
* Por la facilidad de no tener que estar reconstruyendo la imagen cada vez que se le hacían cambios al código para volver a probarlo (y a la vez ahorrarse el uso de un editor pequeño dentro de la imagen, como NANO), mucho del trabajo se probó en un entorno virtual de python (venv) que importaba todo lo necesario similar al contenedor de docker.

* El módulo agregado al `Dockerfile` que sale como `Faker` es para generar datos mock, y también fue muy útil para generar las fechas de manera aleatoria en los datos de los CSV.