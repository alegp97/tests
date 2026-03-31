
El comentario es correcto: el refactor mueve el cálculo de size/modTime a nivel de fichero hoja, pero usa getListStatus(parentPath), por lo que vuelve a listar el mismo directorio repetidamente. En un directorio con N hijos esto provoca N llamadas idénticas. Lo adecuado es deduplicar los parentPath primero y calcular size/modTime una única vez por directorio.
