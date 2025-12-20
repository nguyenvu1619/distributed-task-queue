# Go Learning Guide for Node.js Developers

This guide explains important concepts, dependencies, and Go features used in this Clean Architecture project.

## 📦 Dependencies & Their Purposes

### Core Dependencies (go.mod)

1. **`github.com/labstack/echo/v4`** - Web framework (similar to Express.js)
   - Handles HTTP routing, middleware, request/response
   - Used in `internal/rest/` for API handlers

2. **`github.com/lib/pq`** - PostgreSQL database driver
   - Similar to `pg` in Node.js
   - Used with Go's standard `database/sql` package

3. **`github.com/joho/godotenv`** - Environment variable loader
   - Similar to `dotenv` package in Node.js
   - Loads `.env` files

4. **`github.com/sirupsen/logrus`** - Structured logging library
   - Similar to `winston` or `pino` in Node.js
   - Provides leveled logging (Error, Info, Debug, etc.)

5. **`gopkg.in/go-playground/validator.v9`** - Struct validation
   - Similar to `joi` or `yup` in Node.js
   - Validates struct fields using tags (e.g., `validate:"required"`)

6. **`golang.org/x/sync`** - Additional synchronization primitives
   - Provides `errgroup` for concurrent operations with error handling
   - Used in `article/service.go` for parallel author fetching

7. **`github.com/stretchr/testify`** - Testing utilities
   - Similar to `jest` assertions in Node.js
   - Provides `assert`, `require`, and mocking capabilities

8. **`gopkg.in/DATA-DOG/go-sqlmock.v1`** - SQL mocking for tests
   - Similar to `jest.mock()` for database operations
   - Used in repository tests

9. **`github.com/go-faker/faker/v4`** - Fake data generation
   - Similar to `faker.js` in Node.js
   - Used for generating test data

## 🔑 Key Go Features Used in This Project

### 1. **Interfaces** (Dependency Inversion)
```go
// Interfaces are declared where they're USED, not where they're IMPLEMENTED
type ArticleRepository interface {
    Fetch(ctx context.Context, cursor string, num int64) (res []domain.Article, nextCursor string, err error)
    GetByID(ctx context.Context, id int64) (domain.Article, error)
}
```
- **Key difference from Node.js**: Go uses structural typing (duck typing)
- Interfaces are implicit - if a type has the methods, it implements the interface
- No `implements` keyword needed

### 2. **Struct Tags** (Metadata)
```go
type Article struct {
    ID        int64     `json:"id"`
    Title     string    `json:"title" validate:"required"`
    Content   string    `json:"content" validate:"required"`
}
```
- Similar to decorators in TypeScript, but using backticks
- Used for JSON serialization, validation, database mapping

### 3. **Error Handling**
```go
res, err := a.articleRepo.GetByID(ctx, id)
if err != nil {
    return domain.Article{}, err
}
```
- **Key difference from Node.js**: No try/catch, errors are explicit return values
- Functions return `(result, error)` tuple
- Must check errors explicitly (Go doesn't have exceptions)

### 4. **Context Package** (Request Lifecycle)
```go
ctx := c.Request().Context()
res, err := a.Service.GetByID(ctx, id)
```
- Similar to request context in Express.js middleware
- Used for:
  - Request cancellation/timeouts
  - Passing request-scoped values
  - Database query cancellation

### 5. **Goroutines & Channels** (Concurrency)
```go
// In article/service.go - fillAuthorDetails function
g, ctx := errgroup.WithContext(ctx)
chanAuthor := make(chan domain.Author)

for authorID := range mapAuthors {
    authorID := authorID  // Important: capture loop variable
    g.Go(func() error {
        res, err := a.authorRepo.GetByID(ctx, authorID)
        if err != nil {
            return err
        }
        chanAuthor <- res
        return nil
    })
}
```
- **Goroutines**: Lightweight threads (like async functions, but more powerful)
- **Channels**: Communication between goroutines (like message queues)
- **errgroup**: Manages multiple goroutines and collects errors

### 6. **Package Organization**
```
domain/          - Business entities (like models)
article/         - Service layer (business logic)
internal/        - Private packages (can't be imported by external projects)
  repository/    - Data access layer
  rest/          - HTTP handlers (like controllers)
app/             - Application entry point
```

### 7. **Blank Imports** (Side Effects)
```go
_ "github.com/lib/pq"
```
- The `_` discards the import value
- Used when you only need the package's `init()` function to run
- PostgreSQL driver registers itself with `database/sql` package

### 8. **Defer Statements** (Cleanup)
```go
defer func() {
    err := dbConn.Close()
    if err != nil {
        log.Fatal("got error when closing the DB connection", err)
    }
}()
```
- Similar to `finally` blocks or cleanup in try/finally
- Executes when function returns (even on panic)
- Used for resource cleanup (DB connections, file handles, etc.)

### 9. **Named Return Values**
```go
func (a *Service) GetByID(ctx context.Context, id int64) (res domain.Article, err error) {
    res, err = a.articleRepo.GetByID(ctx, id)
    if err != nil {
        return  // Returns zero values for res and the err
    }
    return res, nil
}
```
- Can name return values in function signature
- `return` without values returns the named variables
- Useful for documentation and cleaner code

### 10. **Method Receivers**
```go
func (a *ArticleHandler) FetchArticle(c echo.Context) error {
    // 'a' is the receiver (like 'this' in a class)
}
```
- Methods attached to types
- `*ArticleHandler` = pointer receiver (can modify the struct)
- `ArticleHandler` = value receiver (works on a copy)

## 🏗️ Architecture Patterns

### Clean Architecture Layers

1. **Domain Layer** (`domain/`)
   - Pure business entities (no dependencies)
   - Similar to TypeScript interfaces/types
   - Contains: `Article`, `Author`, `errors.go`

2. **Repository Layer** (`internal/repository/`)
   - Data access abstraction
   - Implements interfaces defined in service layer
   - Similar to data access layer in Node.js

3. **Service Layer** (`article/`)
   - Business logic
   - Depends on repository interfaces (not implementations)
   - Similar to service classes in Node.js

4. **Delivery Layer** (`internal/rest/`)
   - HTTP handlers
   - Depends on service interfaces
   - Similar to Express.js route handlers

### Dependency Injection Pattern

```go
// In main.go - wiring everything together
authorRepo := postgresRepo.NewAuthorRepository(dbConn)
articleRepo := postgresRepo.NewArticleRepository(dbConn)
svc := article.NewService(articleRepo, authorRepo)  // Dependencies injected
rest.NewArticleHandler(e, svc)  // Service injected into handler
```

- No DI container (unlike NestJS)
- Manual dependency wiring in `main.go`
- Interfaces allow easy swapping of implementations

## 🔄 Key Differences from Node.js

| Node.js | Go |
|---------|-----|
| `async/await` | Goroutines + Channels |
| `Promise.all()` | `errgroup` |
| `try/catch` | Explicit error returns |
| `npm install` | `go get` or `go mod tidy` |
| `package.json` | `go.mod` |
| Classes | Structs + Methods |
| `this` | Method receivers |
| Decorators | Struct tags |
| TypeScript interfaces | Go interfaces (structural) |
| `require()` | `import` |
| Callbacks | Functions as first-class citizens |

## 📚 Important Go Concepts to Learn

### 1. **Pointers**
```go
func (a *Service) Update(ctx context.Context, ar *domain.Article) error
//     ^ pointer receiver    ^ pointer parameter
```
- `*` = pointer (reference to memory address)
- `&` = address of operator
- Similar to references in other languages, but explicit

### 2. **Zero Values**
```go
var article domain.Article  // All fields are zero values
// article.ID = 0
// article.Title = ""
// article.Author = domain.Author{} (zero value struct)
```

### 3. **Slices vs Arrays**
```go
result = make([]domain.Article, 0)  // Slice (dynamic array)
// Similar to JavaScript arrays, but more efficient
```

### 4. **Maps**
```go
mapAuthors := map[int64]domain.Author{}
// Similar to JavaScript objects/Map, but typed
```

### 5. **Type Assertions**
```go
if a, ok := mapAuthors[item.Author.ID]; ok {
    data[index].Author = a
}
// 'ok' tells if the key exists in the map
```

## 🛠️ Development Tools

- **Air**: Hot reload tool (like `nodemon`)
- **Mockery**: Generates mocks for interfaces (like `ts-mockito`)
- **Makefile**: Build automation (like npm scripts)

## 🧪 Testing Approach

- Tests in same package with `_test.go` suffix
- Table-driven tests are common in Go
- Use `testify` for assertions
- Mock interfaces using generated mocks

## 💡 Tips for Node.js Developers

1. **Always check errors** - Go doesn't have exceptions
2. **Use `defer` for cleanup** - Similar to `finally` blocks
3. **Interfaces are implicit** - No need to declare implementation
4. **Context for cancellation** - Use `context.Context` for timeouts/cancellation
5. **Goroutines are cheap** - Unlike threads, you can spawn thousands
6. **Channels for communication** - Use channels instead of shared memory
7. **Explicit is better** - Go favors explicit code over magic

## 📖 Recommended Learning Path

1. **Go Basics**: Variables, functions, structs, methods
2. **Error Handling**: How Go handles errors differently
3. **Interfaces**: Structural typing in Go
4. **Concurrency**: Goroutines, channels, select
5. **Context Package**: Request lifecycle management
6. **Testing**: Table-driven tests, mocking
7. **Package Management**: `go.mod`, `go.sum`

## 🔗 Useful Resources

- [Go Tour](https://go.dev/tour/) - Interactive Go tutorial
- [Effective Go](https://go.dev/doc/effective_go) - Go best practices
- [Go by Example](https://gobyexample.com/) - Code examples
- [Go Blog](https://go.dev/blog/) - Official Go blog






