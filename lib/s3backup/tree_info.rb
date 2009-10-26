require 'yaml'
module S3backup
  class TreeInfo
    attr_reader :fileMap
    def initialize(target)
      @dir_map={}
      @orig = nil
      if target.nil?
        @fileMap = {:file => Hash.new,:symlink => Hash.new,:directory => Hash.new}
      elsif File.directory?(target)
        @orig = {:type=>"directory",:target=>target}
        @fileMap = {:file => Hash.new,:symlink => Hash.new,:directory => Hash.new}
        stat = File.stat(target)
        @fileMap[:directory][target] = {:mtime => stat.mtime, :atime => stat.atime}
        @dir_map[target] = {:mtime => stat.mtime, :atime => stat.atime,:file=>{},:symlink=>{}}
        makeFileMap(target)
      elsif File.file?(target)
        @orig = {:type=>"file",:target=>target}
        load_yaml(File.read(target))
        make_dir_map
      else
        @orig = {:type=>"data",:target=>""}
        load_yaml(target)
        make_dir_map
      end
    end
    def makeFileMap(dir)
      Dir.entries(dir).each do |e|
        if e == "." or e == ".."
          next
        end
        name = dir + "/" + e
        if File.directory?(name)
          stat = File.stat(name)
          @dir_map[name] = {:mtime => stat.mtime, :atime => stat.atime,:file=>{},:symlink=>{}}
          @fileMap[:directory][name] = {:mtime => stat.mtime, :atime => stat.atime}
          makeFileMap(name)
        elsif File.symlink?(name)
          @dir_map[dir][:symlink][name] = {:source => File.readlink(name)}
          @fileMap[:symlink][name] = {:source => File.readlink(name)}
        else
          stat = File.stat(name)
          @dir_map[dir][:file][name] ={:size => stat.size,:date => stat.mtime}
          @fileMap[:file][name] = {:size => stat.size,:date => stat.mtime}
        end
      end
    end
    def make_dir_map
      @fileMap[:directory].each do |k,v|
        @dir_map[k] = {:mtime => v[:mtime], :atime => v[:atime],:file=>{},:symlink=>{}}
      end
      @fileMap[:file].each do |k,v|
        target = @dir_map[File.dirname(k)]
        #不整合だけど適当に作る
        unless target
          S3log.warn("Tree Data isn't correct.#{@orig.inspect}")
          target = {:mtime => DateTime.now.to_s,:atime => DateTime.now.to_s,:file=>{},:symlink=>{}}
          @dir_map[File.dirname(k)] = target
        end
        target[:file][k] = {:size => v[:size], :date => v[:date]}
      end
      @fileMap[:symlink].each do |k,v|
        target = @dir_map[File.dirname(k)]
        #不整合だけど適当に作る
        unless target
          S3log.warn("Tree Data isn't correct.#{@orig.inspect}")
          target = {:mtime => DateTime.now.to_s,:atime => DateTime.now.to_s,:file=>{},:symlink=>{}}
          @dir_map[File.dirname(k)] = target
        end
        target[:symlink][k] = {:source => v[:source]}
      end
    end
    def get_dir_info(name)
      return @dir_map[name]
    end
    def update_dir_map(name,dir_info)
      @dir_map[name][:file] = dir_info[:file]
      @dir_map[name][:symlink] = dir_info[:symlink]
      @dir_map[name][:mtime] = dir_info[:mtime]
      @dir_map[name][:atime] = dir_info[:atime]
    end
    def update_dir(name,dir_info)
      @dir_map[name] = {:file => {},:symlink=>{}} unless @dir_map[name]
      @dir_map[name][:file].each do |k,v|
        @fileMap[:file].delete(k)
      end
      @dir_map[name][:symlink].each do |k,v|
        @fileMap[:symlink].delete(k)
      end
      @fileMap[:directory][name] = {:mtime => dir_info[:mtime],:atime =>dir_info[:atime]}
      dir_info[:file].each do |k,v|
        @fileMap[:file][k] = v
      end
      dir_info[:symlink].each do |k,v|
        @fileMap[:symlink][k] = v
      end
      update_dir_map(name,dir_info)
    end
    def load_yaml(data)
      @fileMap = YAML.load(data)
    end
    def dump_yaml()
      YAML.dump(@fileMap)
    end
    def hierarchie(dir)
      count = dir.count("/")
      tree = []
      @fileMap[:directory].each do |k,v|
        if k.index(dir) != 0
          next
        end
        level = k.count("/") - count
        tree[level] = {} unless tree[level]
        tree[level][k] = v
      end
      return tree
    end
    def diff(target)
      modify_dir_map = {}
      modify_files = []
      modify_links = []

      remove_dirs = target.fileMap[:directory].keys -  @fileMap[:directory].keys
      add_dirs = @fileMap[:directory].keys - target.fileMap[:directory].keys 

      new_info = @fileMap[:file]
      old_info = target.fileMap[:file]

      remove_files = old_info.keys - new_info.keys
      remove_files.each do |f|
        dir = File.dirname(f)
        modify_dir_map[dir] = true
      end
      add_files = new_info.keys - old_info.keys
      add_files.each do |f|
        dir = File.dirname(f)
        modify_dir_map[dir] = true
      end

      new_info.each do |k,v|
        next unless old_info[k]
        if old_info[k][:date] != v[:date] or old_info[k][:size] != v[:size]
          modify_files.push(k)
          dir = File.dirname(k)
          modify_dir_map[dir] = true
        end
      end

      new_info = @fileMap[:symlink]
      old_info = target.fileMap[:symlink]

      remove_links = old_info.keys - new_info.keys
      remove_links.each do |f|
        dir = File.dirname(f)
        modify_dir_map[dir] = true
      end

      add_links = new_info.keys - old_info.keys
      add_links.each do |f|
        dir = File.dirname(f)
        modify_dir_map[dir] = true
      end

      new_info.each do |k,v|
        next unless old_info[k]
        if old_info[k][:source] != v[:source]
          modify_links.push(k)
          dir = File.dirname(k)
          modify_dir_map[dir] = true
        end
      end
      return {
        :directory => {:add => add_dirs,:modify => modify_dir_map.keys - add_dirs - remove_dirs,:remove => remove_dirs},
        :file => {:add => add_files,:modify => modify_files,:remove => remove_files},
        :symlink => {:add => add_links,:modify => modify_links,:remove => remove_links}}
    end
  end
end
