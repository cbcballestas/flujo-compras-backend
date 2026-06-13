---
description: Genera pruebas unitarias completas con JUnit 5 y Mockito para los archivos Java dentro del contexto, cubriendo todos los escenarios posibles con una cobertura mínima del 90%.
---

Analiza todos los archivos Java proporcionados en el contexto y genera pruebas unitarias exhaustivas siguiendo estas reglas:

## Tecnologías requeridas

- **JUnit 5** (`@Test`, `@BeforeEach`, `@AfterEach`, `@ParameterizedTest`, `@ExtendWith`, etc.)
- **Mockito** (`@Mock`, `@InjectMocks`, `@Spy`, `@Captor`, `MockitoExtension`, `when`, `verify`, `doThrow`, `ArgumentCaptor`, etc.)
- **AssertJ** o las aserciones nativas de JUnit 5 (`assertThat`, `assertEquals`, `assertThrows`, `assertNotNull`, etc.)

## Cobertura mínima: 90%

Debes cubrir **todos** los escenarios posibles, incluyendo:

### Escenarios obligatorios por cada método:

1. **Flujo feliz (Happy Path)**: El método funciona correctamente con entradas válidas.
2. **Valores nulos o vacíos**: Pasar `null`, cadenas vacías, listas vacías como parámetros.
3. **Valores límite (Boundary Values)**: Valores mínimos, máximos, cero, negativos donde aplique.
4. **Excepciones esperadas**: Verificar que se lanza la excepción correcta con el mensaje adecuado ante entradas inválidas o estados incorrectos.
5. **Interacciones con dependencias (Mocks)**: Verificar que los colaboradores son invocados con los argumentos correctos usando `verify()`.
6. **Comportamiento cuando el Mock retorna vacío o nulo**: `Optional.empty()`, listas vacías, `null`.
7. **Múltiples llamadas / estado cambiante**: Si el método tiene efectos secundarios, verificar el estado antes y después.

## Estructura de las pruebas

```java
@ExtendWith(MockitoExtension.class)
class NombreClaseTest {

    @Mock
    private DependenciaA dependenciaA;

    @InjectMocks
    private ClaseBajoTest claseBajoTest;

    @BeforeEach
    void setUp() {
        // inicialización común si aplica
    }

    @Test
    @DisplayName("Debe [acción esperada] cuando [condición de entrada]")
    void debeHacerAlgoCuandoCondicion() {
        // Arrange
        // Act
        // Assert
    }
}
```

## Convenciones de nomenclatura

- Nombre del método de prueba: `debe[AccionEsperada]Cuando[Condicion]`
- Usar `@DisplayName` con descripción en español legible.
- Agrupar escenarios relacionados con `@Nested`.

## Reglas adicionales

- **No modifiques** el código fuente de producción.
- Genera el archivo de prueba en el paquete correspondiente bajo `src/test/java`.
- Si la clase usa Kafka, Spring, u otros frameworks, usa las anotaciones de prueba adecuadas (`@SpringBootTest`, `@EmbeddedKafka`, etc.) solo si es estrictamente necesario; prefiere pruebas unitarias puras con mocks.
- Incluye un comentario al inicio del archivo indicando la clase probada y el porcentaje de cobertura estimado.
- Lista al final del archivo (como comentario) todos los escenarios cubiertos.

## Ejemplo de escenarios esperados (para un servicio de órdenes):

```java
// Escenarios cubiertos:
// ✅ Crear orden con datos válidos
// ✅ Crear orden con producto inexistente → lanza OrderException
// ✅ Crear orden con cantidad negativa → lanza IllegalArgumentException
// ✅ Crear orden con lista de ítems vacía → lanza IllegalArgumentException
// ✅ Verificar que se publica evento Kafka al crear orden exitosamente
// ✅ Obtener orden por ID existente → retorna orden
// ✅ Obtener orden por ID inexistente → retorna Optional.empty()
// ✅ Actualizar estado de orden existente
// ✅ Actualizar estado de orden inexistente → lanza OrderNotFoundException
```

Aplica este estándar a **todos los archivos Java** presentes en el contexto del chat.

