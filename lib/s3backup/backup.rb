module S3backup
  class Backup
    def initialize(config)
      check_config(config)
      directories = config["directories"]
      @directories = directories.map{|d| d=~/\/$/ ? d.chop : d}
      begin
        @s3_obj = S3Wrapper.new(config,true)
      rescue => err
        S3log.error(err.backtrace.join("\n")+"\n"+err.message)
        exit(-1)
      end
      @manager = Manager.new(@s3_obj,config)
    end
    def check_config(config)
      if config["log_level"]
        if config["log_level"] =~ /debug|info|warn|error/i
          S3log.set_level(config["log_level"])
        else 
          S3log.error("log_level:#{config['log_level']} is not debug or info or warn or error") 
          exit(-1)
        end
      end
      unless config["directories"]
        S3log.error("directories doesn't exist in config file.")
        exit(-1)
      end
      unless config["directories"].class == Array 
        dir = config["directories"] 
        config["directories"] = Array.new
        config["directories"].push dir
      end
      config["directories"].each do |dir|
        unless File.directory? dir
          S3log.error("#{dir} isn't exist.")
          exit(-1)
        end
        if File.expand_path(dir) != dir
          S3log.error("#{dir.length} must be absolute path.")
          exit(-1)
        end
      end
    end
    def start
      begin
      first_flg=false
      @directories.each do |dir|
        @manager.differential_copy(dir)
      end
      rescue => err
        S3log.error(err.backtrace.join("\n")+"\n"+err.message)
        exit(-1)
      end
    end
  end
end
