<?php

declare(strict_types=1);

namespace Bavix\LaravelClickHouse\Database\Eloquent\Concerns;

use Carbon\CarbonImmutable;
use Illuminate\Database\Eloquent\Concerns\HasAttributes as BaseHasAttributes;
use Illuminate\Support\Carbon;
use DateTimeInterface;
use DateTimeImmutable;

trait HasAttributes
{
    use BaseHasAttributes;

    public function getDates(): array
    {
        return $this->dates ?? [];
    }

    public function getCasts(): array
    {
        return $this->casts ?? [];
    }

    protected function getDateFormat(): string
    {
        return $this->dateFormat ?? 'Y-m-d H:i:s';
    }

    /**
     * Prepare a date for array / JSON serialization.
     *
     * @param  DateTimeInterface  $date
     * @return string
     */
    protected function serializeDate(DateTimeInterface $date)
    {
        return $date instanceof DateTimeImmutable ?
            CarbonImmutable::instance($date)->format('Y-m-d H:i:s') :
            Carbon::instance($date)->format('Y-m-d H:i:s');
    }
}
