require 'optparse'
require 'yaml'
require 's3backup/s3log'

module S3backup
  class CLI
    DEFAULT_CONFIG='./backup.yml'
    def self.execute(stdout, arguments=[])

      # NOTE: the option -p/--path= is given as an example, and should be replaced in your application.
      options = {
        :restore=> false,
        :config_file => DEFAULT_CONFIG,
        :verbose => false,
        :log => nil,
        :output_dir => '.'
      }
      begin 
        parser = OptionParser.new do |opt|
          opt.banner = "Usage: #{File.basename($0)} [Option]"
          opt.on("-r","--restore","restore backup.") {
            options[:restore] = true
          }
          opt.on("-f","--file config",String,"location config file. default: #{DEFAULT_CONFIG}") {|o|
            options[:config_file] = o
          }
          opt.on("-o","--output directory",String,"restore location of directory. default: current directory.") {|o|
            options[:output_dir] = o
          }
          opt.on("-v","--verbose","verbose message to log file"){
            options[:verbose] = true
          }
          opt.on("-l","--log path",String,"path to log file"){|o|
            options[:log] = o
          }
          opt.on("-h","--help","print this message and quit") {
            puts opt.help
            exit 0
          }
          opt.parse!(arguments)
        end
      rescue OptionParser::ParseError => err
        S3log.error(err.message)
        exit 1
      end
      S3log.set_debug(options[:verbose])
      if !File.file?(options[:config_file])
        S3log.error("config #{options[:config_file]} is not exist.")
        exit 1
      end
      if options[:log]
        S3log.set_logfile(File.open(options[:log],"a"))
      end
      if options[:restore]
        require 's3backup/restore'
        if !File.directory?(options[:output_dir])
          S3log.error("output directory #{options[:output_dir]} is not exist.")
          exit 1
        end
        rt = Restore.new(options[:output_dir],YAML.load_file(options[:config_file]))
        rt.start
      else
        require 's3backup/backup'
        bk = Backup.new(YAML.load_file(options[:config_file]))
        bk.start
      end
    end
  end
end
