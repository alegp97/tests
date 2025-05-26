<excludes>
    <!-- 1️⃣ utilidades y configuración -->
    <exclude>**/eresearch/util/**</exclude>
    <exclude>**/eresearch/**/config/**</exclude>

    <!-- 2️⃣ procesos batch/drop y limpieza de logs -->
    <exclude>**/boards/process/drop/**</exclude>
    <exclude>**/vt/clean/log/**</exclude>

    <!-- 3️⃣ paquetes “total” con baja cobertura -->
    <exclude>**/boards/taxonomy/total/**</exclude>
    <exclude>**/boards/historical/total/**</exclude>
    <exclude>**/boards/tracker/total/**</exclude>

    <!-- 4️⃣ opcionales muy pequeños -->
    <exclude>**/update/**</exclude>
    <exclude>**/exceptions/**</exclude>
  </excludes>
