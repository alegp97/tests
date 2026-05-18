Los cached tokens de OpenAI NO deben sumarse encima de los input tokens. Son un subconjunto de los prompt/input tokens.

Ejemplo:
OpenAI devuelve:
- input_tokens = 1000
- cached_tokens = 500
- output_tokens = 200
- reasoning_tokens = 0

La normalización correcta debe ser:
- INPUT_TOKENS_UNCACHED = 1000 - 500 = 500
- INPUT_TOKENS_CACHED = 500
- OUTPUT_TOKENS = 200
- REASONING_TOKENS = 0

No quiero un diseño donde INPUT_TOKENS=1000 y CACHED_INPUT_TOKENS=500 se sumen como unidades independientes, porque eso produce doble conteo. Para pricing, las unidades deben ser mutuamente excluyentes.

Fórmula de coste:
cost =
  uncached_input_tokens * input_price
+ cached_input_tokens   * cached_input_price
+ output_tokens         * output_price
+ reasoning_tokens      * reasoning_price, si aplica

La estimación inicial antes de llamar al modelo puede ser conservadora:
- contar history + prompt + system
- estimar todo como input normal no cacheado
- no asumir cached tokens antes de recibir el usage real
- luego corregir con el coste real al terminar la llamada
