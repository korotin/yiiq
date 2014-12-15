<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains signal dispatcher class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.util
 */

namespace Yiiq\util;

use Yiiq\Yiiq;
use Yiiq\base\Component;

/**
 * Signal dispatcher class.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 */
class SignalDispatcher extends Component
{
    /**
     * Signal handlers.
     *
     * @var array
     */
    protected $handlers = [];

    /**
     * Whether any signal handled already.
     *
     * @var boolean
     */
    protected $signalHandled = false;

    /**
     * Handle specified signal.
     *
     * @param integer $signal
     */
    public function handle($signal)
    {
        $this->signalHandled = true;
        if (!isset($this->handlers[$signal])) {
            return;
        }

        foreach ($this->handlers[$signal] as $callback) {
            $callback();
        }
    }

    /**
     * Add callback for signal.
     * Multiple callbacks may be added.
     *
     * @param  integer          $signal
     * @param  mixed            $callback
     * @return SignalDispatcher
     */
    public function on($signal, $callback)
    {
        if (!isset($this->handlers[$signal])) {
            $this->handlers[$signal] = [];
        }

        $this->handlers[$signal][] = $callback;

        if (count($this->handlers[$signal]) === 1) {
            pcntl_signal($signal, function () use ($signal) {
                $this->handle($signal);
            });
        }

        return $this;
    }

    /**
     * Wait some time for signal.
     * If signal received, it will be correclty handled.
     */
    public function wait()
    {
        $siginfo = [];
        $signal = pcntl_sigtimedwait(
            array_keys($this->handlers),
            $siginfo,
            0,
            pow(10, 9) * 0.01
        );

        $this->handle($signal);
    }

    /**
     * Dispatch all signals.
     */
    public function dispatch()
    {
        do {
            $this->signalHandled = false;
            pcntl_signal_dispatch();
        } while ($this->signalHandled);
    }
}
