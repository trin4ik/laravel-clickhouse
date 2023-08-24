<?php

declare(strict_types=1);

namespace Bavix\LaravelClickHouse\Database\Eloquent;

use ArrayAccess;
use Illuminate\Contracts\Routing\UrlRoutable;
use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Database\Eloquent\Relations\HasManyThrough;
use Illuminate\Database\Eloquent\Relations\Relation;
use JsonSerializable;
use Bavix\LaravelClickHouse\Database\Connection;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Contracts\Support\Jsonable;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\ConnectionResolverInterface;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Concerns\GuardsAttributes;
use Illuminate\Database\Eloquent\Concerns\HasEvents;
use Illuminate\Database\Eloquent\Concerns\HasRelationships;
use Illuminate\Database\Eloquent\Concerns\HidesAttributes;
use Illuminate\Database\Eloquent\JsonEncodingException;
use Illuminate\Database\Eloquent\MassAssignmentException;
use Illuminate\Support\Str;
use Tinderbox\ClickhouseBuilder\Query\Grammar;
use Bavix\LaravelClickHouse\Database\Query\Builder as QueryBuilder;

/**
 * Class Model.
 */
abstract class Model implements ArrayAccess, Arrayable, Jsonable, JsonSerializable, UrlRoutable
{
    use Concerns\HasAttributes;
    use Concerns\Common;
    use HasEvents;
    use HasRelationships;
    use HidesAttributes;
    use GuardsAttributes;

    /**
     * The connection name for the model.
     */
    protected string $connection = 'bavix::clickhouse';

    /**
     * The table associated with the model.
     */
    protected string $table;

    /**
     * The primary key for the model.
     */
    protected string $primaryKey = 'id';

    /**
     * The "type" of primary key.
     */
    protected string $keyType = 'int';

    /**
     * The relations to eager load on every query.
     */
    protected array $with = [];

    /**
     * The relationship counts that should be eager loaded on every query.
     */
    protected array $withCount = [];

    /**
     * The number of models to return for pagination.
     */
    protected int $perPage = 15;

    /**
     * Indicates if the model was inserted during the current request lifecycle.
     */
    public bool $wasRecentlyCreated = false;

    /**
     * Indicates if an exception should be thrown when trying to access a missing attribute on a retrieved model.
     *
     * @var bool
     */
    protected static bool $modelsShouldPreventAccessingMissingAttributes = false;

    /**
     * Indicates if the model exists.
     */
    public bool $exists = false;

    /**
     * The connection resolver instance.
     */
    protected static ConnectionResolverInterface $resolver;

    /**
     * The event dispatcher instance.
     */
    protected static Dispatcher $dispatcher;

    /**
     * The array of booted models.
     */
    protected static array $booted = [];

    /**
     * Create a new Eloquent model instance.
     */
    public function __construct (array $attributes = [])
    {
        $this->bootIfNotBooted();

        $this->syncOriginal();

        $this->fill($attributes);
    }

    /**
     * Check if the model needs to be booted and if so, do it.
     */
    protected function bootIfNotBooted (): void
    {
        if (!isset(static::$booted[static::class])) {
            static::$booted[static::class] = true;

            $this->fireModelEvent('booting', false);

            static::boot();

            $this->fireModelEvent('booted', false);
        }
    }

    /**
     * The "booting" method of the model.
     */
    protected static function boot (): void
    {
        static::bootTraits();
    }

    /**
     * Boot all the bootable traits on the model.
     */
    protected static function bootTraits (): void
    {
        $class = static::class;

        foreach (class_uses_recursive($class) as $trait) {
            if (method_exists($class, $method = 'boot' . class_basename($trait))) {
                forward_static_call([$class, $method]);
            }
        }
    }

    /**
     * Clear the list of booted models so they will be re-booted.
     */
    public static function clearBootedModels (): void
    {
        static::$booted = [];
    }

    /**
     * Fill the model with an array of attributes.
     *
     * @throws \Illuminate\Database\Eloquent\MassAssignmentException
     */
    public function fill (array $attributes): static
    {
        $totallyGuarded = $this->totallyGuarded();

        foreach ($this->fillableFromArray($attributes) as $key => $value) {
            $key = $this->removeTableFromKey($key);

            // The developers may choose to place some attributes in the "fillable" array
            // which means only those attributes may be set through mass assignment to
            // the model, and all others will just get ignored for security reasons.
            if ($this->isFillable($key)) {
                $this->setAttribute($key, $value);
            } elseif ($totallyGuarded) {
                throw new MassAssignmentException($key);
            }
        }

        return $this;
    }

    /**
     * Fill the model with an array of attributes. Force mass assignment.
     */
    public function forceFill (array $attributes): static
    {
        return static::unguarded(function () use ($attributes) {
            return $this->fill($attributes);
        });
    }

    /**
     * Remove the table name from a given key.
     */
    protected function removeTableFromKey (string $key): string
    {
        return Str::contains($key, '.') ? last(explode('.', $key)) : $key;
    }

    /**
     * Create a new instance of the given model.
     */
    public function newInstance (array $attributes = [], bool $exists = false): static
    {
        // This method just provides a convenient way for us to generate fresh model
        // instances of this current model. It is particularly useful during the
        // hydration of new objects via the Eloquent query builder instances.
        $model = new static((array)$attributes);

        $model->exists = $exists;

        $model->setConnection(
            $this->getConnectionName()
        );

        return $model;
    }

    /**
     * Create a new model instance that is existing.
     */
    public function newFromBuilder (array $attributes = [], ?string $connection = null): static
    {
        $model = $this->newInstance([], true);

        $model->setRawAttributes($attributes, true);

        $model->setConnection($connection ?: $this->getConnectionName());

        $model->fireModelEvent('retrieved', false);

        return $model;
    }

    /**
     * Get all the models from the database.
     */
    public static function all (): Collection
    {
        return (new static())->newQuery()->get();
    }

    /**
     * Begin querying a model with eager loading.
     */
    public static function with (array|string $relations): Builder|static
    {
        return (new static())->newQuery()->with(
            is_string($relations) ? func_get_args() : $relations
        );
    }

    public static function query (): Builder
    {
        return (new static())->newQuery();
    }

    public function newQuery (): Builder
    {
        return $this->newQueryWithoutScopes();
    }

    public function newQueryWithoutScopes (): Builder
    {
        $builder = $this->newEloquentBuilder($this->newBaseQueryBuilder());

        // Once we have the query builders, we will set the model instances so the
        // builder can easily access any information it may need from the model
        // while it is constructing and executing various queries against it.
        return $builder->setModel($this)
            ->with($this->with);
    }

    public function newQueryWithoutScope (): Builder
    {
        return $this->newQuery();
    }

    public function newEloquentBuilder (QueryBuilder $query): Builder
    {
        return new Builder($query);
    }

    protected function newBaseQueryBuilder (): QueryBuilder
    {
        /** @var Connection $connection */
        $connection = $this->getConnection();
        return new QueryBuilder($connection, new Grammar());
    }

    /**
     * Create a new Eloquent Collection instance.
     */
    public function newCollection (array $models = []): Collection
    {
        return new Collection($models);
    }

    /**
     * Convert the model instance to an array.
     */
    public function toArray (): array
    {
        return $this->attributesToArray();
    }

    /**
     * Convert the model instance to JSON.
     *
     * @throws \Illuminate\Database\Eloquent\JsonEncodingException
     */
    public function toJson ($options = 0): string
    {
        $json = json_encode($this->jsonSerialize(), $options);

        if (JSON_ERROR_NONE !== json_last_error()) {
            throw JsonEncodingException::forModel($this, json_last_error_msg());
        }

        return $json;
    }

    /**
     * Convert the object into something JSON serializable.
     */
    public function jsonSerialize (): array
    {
        return $this->toArray();
    }

    /**
     * Get the database connection for the model.
     */
    public function getConnection (): ConnectionInterface
    {
        return static::resolveConnection($this->getConnectionName());
    }

    /**
     * Get the current connection name for the model.
     */
    public function getConnectionName (): string
    {
        return $this->connection;
    }

    /**
     * Set the connection associated with the model.
     */
    public function setConnection (string $name): static
    {
        $this->connection = $name;

        return $this;
    }

    /**
     * Resolve a connection instance.
     */
    public static function resolveConnection (?string $connection = null): ConnectionInterface
    {
        return static::getConnectionResolver()->connection($connection);
    }

    /**
     * Get the connection resolver instance.
     */
    public static function getConnectionResolver (): ConnectionResolverInterface
    {
        return static::$resolver;
    }

    /**
     * Set the connection resolver instance.
     */
    public static function setConnectionResolver (ConnectionResolverInterface $resolver): void
    {
        static::$resolver = $resolver;
    }

    /**
     * Get the table associated with the model.
     */
    public function getTable (): string
    {
        if (!isset($this->table)) {
            $this->setTable(str_replace(
                '\\',
                '',
                Str::snake(Str::plural(class_basename($this)))
            ));
        }

        return $this->table;
    }

    /**
     * Set the table associated with the model.
     */
    public function setTable (string $table): static
    {
        $this->table = $table;

        return $this;
    }

    /**
     * Get the primary key for the model.
     */
    public function getKeyName (): string
    {
        return $this->primaryKey;
    }

    /**
     * Set the primary key for the model.
     */
    public function setKeyName (string $key): static
    {
        $this->primaryKey = $key;

        return $this;
    }

    /**
     * Get the table qualified key name.
     */
    public function getQualifiedKeyName (): string
    {
        return $this->getTable() . '.' . $this->getKeyName();
    }

    /**
     * Get the pk key type.
     */
    public function getKeyType (): string
    {
        return $this->keyType;
    }

    /**
     * Set the data type for the primary key.
     */
    public function setKeyType (string $type): static
    {
        $this->keyType = $type;

        return $this;
    }

    /**
     * Get the value of the model's primary key.
     */
    public function getKey (): mixed
    {
        return $this->getAttribute($this->getKeyName());
    }

    /**
     * Get the default foreign key name for the model.
     */
    public function getForeignKey (): string
    {
        return Str::snake(class_basename($this)) . '_' . $this->primaryKey;
    }

    /**
     * Get the number of models to return per page.
     */
    public function getPerPage (): int
    {
        return $this->perPage;
    }

    /**
     * Set the number of models to return per page.
     */
    public function setPerPage (int $perPage): static
    {
        $this->perPage = $perPage;

        return $this;
    }

    /**
     * Get the value of the model's route key.
     */
    public function getRouteKey(): mixed
    {
        return $this->getAttribute($this->getRouteKeyName());
    }

    /**
     * Get the route key for the model.
     */
    public function getRouteKeyName(): string
    {
        return $this->getKeyName();
    }

    /**
     * Retrieve the model for a bound value.
     *
     * @param mixed $value
     * @param string|null $field
     */
    public function resolveRouteBinding($value, $field = null): ?Model
    {
        return $this->resolveRouteBindingQuery($this, $value, $field)->first();
    }

    /**
     * Retrieve the model for a bound value.
     */
    public function resolveSoftDeletableRouteBinding(mixed $value, ?string $field = null): ?Model
    {
        return $this->resolveRouteBindingQuery($this, $value, $field)->withTrashed()->first();
    }

    /**
     * Retrieve the child model for a bound value.
     *
     * @param string $childType
     * @param mixed $value
     * @param string|null $field
     */
    public function resolveChildRouteBinding($childType, $value, $field): ?Model
    {
        return $this->resolveChildRouteBindingQuery($childType, $value, $field)->first();
    }

    /**
     * Retrieve the child model for a bound value.
     */
    public function resolveSoftDeletableChildRouteBinding(string $childType, mixed $value, ?string $field = null): ?Model
    {
        return $this->resolveChildRouteBindingQuery($childType, $value, $field)->withTrashed()->first();
    }

    /**
     * Retrieve the child model query for a bound value.
     */
    protected function resolveChildRouteBindingQuery(string $childType, mixed $value, ?string $field = null): Relation|Model
    {
        $relationship = $this->{$this->childRouteBindingRelationshipName($childType)}();

        $field = $field ?: $relationship->getRelated()->getRouteKeyName();

        if ($relationship instanceof HasManyThrough ||
            $relationship instanceof BelongsToMany) {
            $field = $relationship->getRelated()->getTable().'.'.$field;
        }

        return $relationship instanceof Model
            ? $relationship->resolveRouteBindingQuery($relationship, $value, $field)
            : $relationship->getRelated()->resolveRouteBindingQuery($relationship, $value, $field);
    }

    /**
     * Retrieve the child route model binding relationship name for the given child type.
     */
    protected function childRouteBindingRelationshipName(string $childType): string
    {
        return Str::plural(Str::camel($childType));
    }

    /**
     * Retrieve the model for a bound value.
     */
    public function resolveRouteBindingQuery(Relation|Model $query, mixed $value, ?string $field = null): Builder
    {
        return $query->where($field ?? $this->getRouteKeyName(), $value);
    }

    /**
     * Dynamically retrieve attributes on the model.
     */
    public function __get (string $key): mixed
    {
        return $this->getAttribute($key);
    }

    /**
     * Dynamically set attributes on the model.
     */
    public function __set (string $key, mixed $value): void
    {
        $this->setAttribute($key, $value);
    }

    /**
     * Determine if the given attribute exists.
     */
    public function offsetExists (mixed $offset): bool
    {
        return $this->getAttribute($offset) !== null;
    }

    /**
     * Get the value for a given offset.
     */
    #[\ReturnTypeWillChange]
    public function offsetGet (mixed $offset): mixed
    {
        return $this->getAttribute($offset);
    }

    /**
     * Set the value for a given offset.
     */
    public function offsetSet (mixed $offset, mixed $value): void
    {
        $this->setAttribute($offset, $value);
    }

    /**
     * Unset the value for a given offset.
     */
    public function offsetUnset (mixed $offset): void
    {
        unset($this->attributes[$offset]);
    }

    /**
     * Determine if accessing missing attributes is disabled.
     */
    public static function preventsAccessingMissingAttributes (): bool
    {
        return static::$modelsShouldPreventAccessingMissingAttributes;
    }

    /**
     * Determine if an attribute or relation exists on the model.
     */
    public function __isset (string $key): bool
    {
        return $this->offsetExists($key);
    }

    /**
     * Unset an attribute on the model.
     */
    public function __unset (string $key): void
    {
        $this->offsetUnset($key);
    }

    /**
     * Handle dynamic method calls into the model.
     */
    public function __call (string $method, array $parameters): mixed
    {
        return $this->newQuery()->$method(...$parameters);
    }

    /**
     * Handle dynamic static method calls into the method.
     */
    public static function __callStatic (string $method, array $parameters): mixed
    {
        return (new static)->$method(...$parameters);
    }
}
