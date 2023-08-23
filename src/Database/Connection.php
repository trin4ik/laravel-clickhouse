<?php

declare(strict_types=1);

namespace Bavix\LaravelClickHouse\Database;

use Bavix\LaravelClickHouse\Database\Query\Builder;
use Bavix\LaravelClickHouse\Database\Query\Pdo;
use Tinderbox\ClickhouseBuilder\Query\Grammar;

class Connection extends \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection
{
    public function query(): Builder
    {
        return new Builder($this, new Grammar());
    }

    public function getPdo(): Pdo
    {
        return app(Pdo::class);
    }
}
