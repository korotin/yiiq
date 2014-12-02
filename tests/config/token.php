<?php

// Define paratest token.
function getTestToken()
{
    $token = getenv('TEST_TOKEN');
    if (!$token) {
        return md5('no_token');
    } elseif (strlen($token) !== 32) {
        return md5($token.'_'.microtime(true));
    }

    return $token;
}

define('TEST_TOKEN', getTestToken());
