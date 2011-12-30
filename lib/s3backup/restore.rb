require 'sqlite3'
module S3backup
  class Restore
    def initialize(output_dir,config)
      check_config(config)
      @output_dir = output_dir
      @directories = config["directories"]
      begin
        @s3_obj = S3Wrapper.new(config,false)
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
      if config["directories"] 
        if config["directories"].class != Array 
          dir = config["directories"] 
          config["directories"] = Array.new
          config["directories"].push dir
        end
        config["directories"] = config["directories"].map{|d| d=~/\/$/ ? d.chop : d}
      end
    end
    def start
      S3log.error("directories is not defined") unless @directories
      begin
        @directories.each do |dir|
          @manager.restore(dir,@output_dir)
        end
      rescue => err
        S3log.error(err.backtrace.join("\n")+"\n"+err.message)
        exit(-1)
      end
    end
  end
end
