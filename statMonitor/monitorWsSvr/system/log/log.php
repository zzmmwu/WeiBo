<?php
require_once "system/load_config.php";

if ( ! function_exists('is_php'))
{
	/**
	 * Determines if the current version of PHP is equal to or greater than the supplied value
	 *
	 * @param	string
	 * @return	bool	TRUE if the current version is $version or higher
	 */
	function is_php($version)
	{
		static $_is_php;
		$version = (string) $version;

		if ( ! isset($_is_php[$version]))
		{
			$_is_php[$version] = version_compare(PHP_VERSION, $version, '>=');
		}

		return $_is_php[$version];
	}
}

if ( ! function_exists('is_really_writable'))
{
	/**
	 * Tests for file writability
	 *
	 * is_writable() returns TRUE on Windows servers when you really can't write to
	 * the file, based on the read-only attribute. is_writable() is also unreliable
	 * on Unix servers if safe_mode is on.
	 *
	 * @link	https://bugs.php.net/bug.php?id=54709
	 * @param	string
	 * @return	bool
	 */
	function is_really_writable($file)
	{
		// If we're on a Unix server with safe_mode off we call is_writable
		if (DIRECTORY_SEPARATOR === '/' && (is_php('5.4') OR ! ini_get('safe_mode')))
		{
			return is_writable($file);
		}

		/* For Windows servers and safe_mode "on" installations we'll actually
		 * write a file then read it. Bah...
		 */
		if (is_dir($file))
		{
			$file = rtrim($file, '/').'/'.md5(mt_rand());
			if (($fp = @fopen($file, 'ab')) === FALSE)
			{
				return FALSE;
			}

			fclose($fp);
			@chmod($file, 0777);
			@unlink($file);
			return TRUE;
		}
		elseif ( ! is_file($file) OR ($fp = @fopen($file, 'ab')) === FALSE)
		{
			return FALSE;
		}

		fclose($fp);
		return TRUE;
	}
}

class Log {

    /**
     * Path to save log files
     *
     * @var string
     */
    protected $_log_path;
    
    protected $_sub_path;

    /**
     * File permissions
     *
     * @var int
     */
    protected $_file_permissions = 0644;

    /**
     * Level of logging
     *
     * @var int
     */
    protected $_threshold = 1;

    /**
     * Array of threshold levels to log
     *
     * @var array
     */
    protected $_threshold_array = array();

    /**
     * Format of timestamp for log files
     *
     * @var string
     */
    protected $_date_fmt = 'Y-m-d H:i:s.u';
    
    protected $_file_prefix = "";
    
    protected $_file_idx = "0";
    
    protected $_file_size = 104857600;

    /**
     * Filename extension
     *
     * @var string
     */
    protected $_file_ext;

    /**
     * Whether or not the logger can write to the log files
     *
     * @var bool
     */
    protected $_enabled = TRUE;

    /**
     * Predefined logging levels
     *
     * @var array
     */
    protected $_levels = array('ERROR' => 1, 'DEBUG' => 2, 'INFO' => 3, 'ALL' => 4);

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @return  void
     */
    public function __construct($file_prefix = "")
    {
        ini_set('date.timezone','Asia/Shanghai');
        $load_config = new load_config();
        $config = $load_config->fc_load_config("system/conf/config.ini");
        $this->_log_path = ($config['log_path'] != '') ? $config['log_path'] : 'logs/';
        $this->_sub_path = date("Y-m-d");
        $this->_file_ext = (isset($config['log_file_extension']) && $config['log_file_extension'] !== '')
            ? ltrim($config['log_file_extension'], '.') : 'log';

        file_exists($this->_log_path) OR mkdir($this->_log_path, 0755, TRUE);        
     
        $this->_file_prefix = $file_prefix;

        $this->_file_idx = 0;
        while (true)
        {
            if (file_exists($this->_log_path.$this->_sub_path.'/'.$this->_file_prefix.".".$this->_file_ext.".".$this->_file_idx))
            {
                $cur_size = filesize($this->_log_path.$this->_sub_path.'/'.$this->_file_prefix.".".$this->_file_ext.".".$this->_file_idx);
                if ($cur_size > $this->_file_size)
                    $this->_file_idx++;
                else
                    break;
            }
            else
            {
                break;
            }
        }

        if (is_numeric($config['log_threshold']))
        {
            $this->_threshold = (int) $config['log_threshold'];
        }
        elseif (is_array($config['log_threshold']))
        {
            $this->_threshold = 0;
            $this->_threshold_array = array_flip($config['log_threshold']);
        }
        
        if (!empty($config['log_file_size']) && is_numeric($config['log_file_size']))
        {
            $this->_file_size = (int)$config['log_file_size'];
        }
        

        if ( ! empty($config['log_date_format']))
        {
            $this->_date_fmt = $config['log_date_format'];
        }

        if ( ! empty($config['log_file_permissions']) && is_int($config['log_file_permissions']))
        {
            $this->_file_permissions = $config['log_file_permissions'];
        }
    }

    // --------------------------------------------------------------------

    /**
     * Write Log File
     *
     * Generally this function will be called using the global log_message() function
     *
     * @param   string  the error level: 'error', 'debug' or 'info'
     * @param   string  the error message
     * @return  bool
     */
    public function write_log($level, $msg)
    {

        $level = strtoupper($level);

        if (( ! isset($this->_levels[$level]) OR ($this->_levels[$level] > $this->_threshold))
            && ! isset($this->_threshold_array[$this->_levels[$level]]))
        {
            return FALSE;
        }
        
        if ($this->_sub_path == date("Y-m-d"))
        {
            if (file_exists($this->_log_path.$this->_sub_path.'/'.$this->_file_prefix.".".$this->_file_ext.".".$this->_file_idx))
            {
                $cur_size = filesize($this->_log_path.$this->_sub_path.'/'.$this->_file_prefix.".".$this->_file_ext.".".$this->_file_idx);
                if ($cur_size > $this->_file_size)
                    $this->_file_idx++;
            }
        }
        else
        {
            $this->_sub_path = date("Y-m-d");
            $this->_file_idx = 0;
        }
        del_dir_file($this->_log_path.date("Y-m-d",strtotime("-5 day")),true);
        file_exists($this->_log_path.$this->_sub_path) OR mkdir($this->_log_path.$this->_sub_path, 0755, TRUE);

        if ( ! is_dir($this->_log_path) OR ! is_really_writable($this->_log_path))
        {
            $this->_enabled = FALSE;
            return false;
        }        

        $filepath = $this->_log_path.$this->_sub_path.'/'.$this->_file_prefix.".".$this->_file_ext.".".$this->_file_idx;

        $message = '';

        if ( ! $fp = @fopen($filepath, 'ab'))
        {
            return FALSE;
        }

        // Instantiating DateTime with microseconds appended to initial date is needed for proper support of this format
        if (strpos($this->_date_fmt, 'u') !== FALSE)
        {
            $microtime_full = microtime(TRUE);
            $microtime_short = sprintf("%06d", ($microtime_full - floor($microtime_full)) * 1000000);
            $date = new DateTime(date('Y-m-d H:i:s.'.$microtime_short, $microtime_full));
            $date = $date->format($this->_date_fmt);
        }
        else
        {
            $date = date($this->_date_fmt);
        }

        $bt = debug_backtrace(false);
        $filename = '';
        $linenumber = '';
        if (isset($bt[1])) {
            $filename = $bt[1]['file'];
            $linenumber = $bt[1]['line'];
        }

        $message .= "{$level}|{$date}|{$filename}:{$linenumber}|{$msg}\n";

        flock($fp, LOCK_EX);

        for ($written = 0, $length = strlen($message); $written < $length; $written += $result)
        {
            if (($result = fwrite($fp, substr($message, $written))) === FALSE)
            {
                break;
            }
        }

        flock($fp, LOCK_UN);
        fclose($fp);

        if (isset($newfile) && $newfile === TRUE)
        {
            chmod($filepath, $this->_file_permissions);
        }

        return is_int($result);
    }
}

function log_message($level, $message, $name='mina_auth')
{
    static $_log;

    if ($_log == NULL)
    {
        // references cannot be directly assigned to static variables, so we use an array
        $_log[0] = new Log($name);
    }

    $_log[0]->write_log($level, $message);
}

function stat_log($level, $message)
{
    static $_log_stat;

    if ($_log_stat == NULL)
    {
        $pid = getmypid();
        // references cannot be directly assigned to static variables, so we use an array
        $_log_stat[0] = new Log('stat'.$pid);
    }

    $_log_stat[0]->write_log($level, $message);
}


function del_dir_file($path, $delDir = FALSE) {
    if(file_exists($path)){
        $handle = opendir($path);
        if ($handle) {
            while (false !== ( $item = readdir($handle) )) {
                if ($item != "." && $item != "..")
                    is_dir("$path/$item") ? del_dir_file("$path/$item", $delDir) : unlink("$path/$item");
            }
            closedir($handle);
            if ($delDir)
                return rmdir($path);
        }else {
            if (file_exists($path)) {
                return unlink($path);
            } else {
                return FALSE;
            }
        }
    }
}