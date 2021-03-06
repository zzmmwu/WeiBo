<?php

// autoload_static.php @generated by Composer

namespace Composer\Autoload;

class ComposerStaticInitc56d94c4e60c66be1fc17cd2e69fb12b
{
    public static $prefixLengthsPsr4 = array (
        'W' => 
        array (
            'Workerman\\MySQL\\' => 16,
            'Workerman\\' => 10,
        ),
    );

    public static $prefixDirsPsr4 = array (
        'Workerman\\MySQL\\' => 
        array (
            0 => __DIR__ . '/..' . '/workerman/mysql/src',
        ),
        'Workerman\\' => 
        array (
            0 => __DIR__ . '/../..' . '/',
        ),
    );

    public static function getInitializer(ClassLoader $loader)
    {
        return \Closure::bind(function () use ($loader) {
            $loader->prefixLengthsPsr4 = ComposerStaticInitc56d94c4e60c66be1fc17cd2e69fb12b::$prefixLengthsPsr4;
            $loader->prefixDirsPsr4 = ComposerStaticInitc56d94c4e60c66be1fc17cd2e69fb12b::$prefixDirsPsr4;

        }, null, ClassLoader::class);
    }
}
