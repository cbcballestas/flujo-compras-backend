---
description: Agrega JavaDoc a todos los métodos de la clase o archivo Java seleccionado, describiendo detalladamente la funcionalidad que realiza cada método, sus parámetros, valores de retorno y excepciones lanzadas.
---

Analiza el archivo Java proporcionado y agrega documentación JavaDoc completa a cada método que no la tenga, o mejora la existente si está incompleta. Sigue estas reglas:

1. **Descripción del método**: Escribe una descripción clara y detallada de qué hace el método, su propósito dentro de la clase y cómo contribuye a la lógica del negocio.
2. **`@param`**: Documenta cada parámetro indicando su tipo, nombre y qué representa.
3. **`@return`**: Si el método retorna un valor (distinto de `void`), describe qué contiene ese valor de retorno.
4. **`@throws`**: Si el método lanza excepciones (declaradas o documentadas), indícalas con una descripción de cuándo ocurren.
5. **Constructores**: Documenta también los constructores explicando qué inicializa.
6. **Idioma**: Escribe la documentación en español.
7. **No modifiques** la lógica ni el código existente, solo agrega o actualiza los comentarios JavaDoc.

Ejemplo de JavaDoc esperado:

```java
/**
 * Procesa el pago de una orden verificando el saldo disponible del cliente
 * y registrando la transacción en el sistema.
 *
 * @param orderId  identificador único de la orden a procesar
 * @param amount   monto total a cobrar en la moneda base del sistema
 * @return {@code true} si el pago fue procesado exitosamente, {@code false} en caso contrario
 * @throws PaymentException si el saldo del cliente es insuficiente o la orden no existe
 */
public boolean processPayment(String orderId, double amount) { ... }
```

Aplica este estándar a todos los métodos del archivo Java activo en el editor.
