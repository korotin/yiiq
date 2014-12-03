<?php
/**
 * Yiiq - background job queue manager for Yii
 *
 * This file contains token determining logic.
 *
 * @author  Martin Stolz <herr.offizier@gmail.com>
 * @package yiiq.tests.config
 */

// Define paratest token.
function getTestToken()
{
    $token = getenv('TEST_TOKEN');
    if (!$token) {
        return md5('no_token');
    } elseif (strlen($token) !== 32) {
        return md5($token.'_'.rand());
    }

    return $token;
}

define('TEST_TOKEN', getTestToken());
