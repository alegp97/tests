Borrar el fichero/carpeta de origen en DBFS (si el origen está en DBFS y no es tu máquina local)

Para un fichero:
databricks fs rm dbfs:/path/origen/<fichero>

Para una carpeta (recursivo):
databricks fs rm --recursive dbfs:/path/origen/<carpeta>
