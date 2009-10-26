require 'cgi'
require 'tempfile'
require 'fileutils'
require 's3backup/s3log'
require 's3backup/tree_info'
module S3backup
  class Manager
    DEFAULT_BUF_READ_SIZE=1024*1024*128
    def initialize(target,config)
      @target = target
      set_config(config)
    end
    def set_config(config)
      if config["password"] and config["password"] != ""
        unless config["salt"] 
          raise "salt doesn't exist in config file.\n"
        end
        unless config["salt"] =~ /[0-9A-Fa-f]{16}/
          raise "salt format shoud be HexString and length should be 16.\n"
        end
        if config["BUF_SIEZE"]
          size=config["BUF_SIEZE"]
          if size > 1000*1000*1000*5
            raise "BUF_SIZE must be less than 5G"
          end
        else
          @buf_size = DEFAULT_BUF_READ_SIZE
        end
        @aes = Crypt.new(config["password"],config["salt"])
      end
    end
    #指定されたディレクトリをtar gzip形式で圧縮する
    def to_tgz(path,dir)
      #サブディレクトリを圧縮の対象外にする。
      sub_dir = []
      Dir.foreach(dir) do |file|
        next if /^\.+$/ =~ file 
        sub_dir.push(file) if File.directory?(dir+"/"+file)
      end
      exclude = ""
      exclude = exclude + " --exclude=" + sub_dir.join("  --exclude=") if sub_dir.length != 0
      cmd = "(cd #{File.dirname(dir)};tar -czvf #{path} #{exclude} #{File.basename(dir)} > /dev/null 2>&1)"
      S3log.debug(cmd)
      system(cmd) 
      unless $?.success?
        raise "feiled #{cmd} execute. #{$?.inspect}"
      end
    end
    def from_tgz(path,dir)
      cmd = "tar -xzvf #{path} -C #{dir} > /dev/null 2>&1"
      S3log.debug(cmd)
      system(cmd) 
      unless $?.success?
        raise "feiled #{cmd} execute. #{$?.inspect}"
      end
    end
    def get_chain(key)
      data = nil
      data_set = nil
      i=1
      if @aes
        key = @aes.encrypt(key)
      end
      while 1
        key_name = i.to_s()+"_"+key
        data = @target.get(key_name)
        if data == nil
          break
        end
        if i==1
          data_set = ''
        end
        if @aes
          data = @aes.decrypt(data)
        end
        data_set += data
        i+=1
      end
      return data_set
    end
    def get_directory(dir,out_dir)
      data = get_chain(dir)
      tmp = Tempfile.open("s3backup")
      tmp.write(data)
      tmp.close
      #tgzのファイルをcur_dirに展開
      from_tgz(tmp.path,out_dir)
      tmp.close(true)
    end
    def get_directories(dirs,prefix,output_dir)
      prefix_len = prefix.length
      dirs.each do |dir|
        parent = File.dirname(dir)
        p_len = parent.length
        relative_path = parent.slice(prefix_len,p_len - prefix_len)
        cur_dir = output_dir + relative_path
        get_directory(dir,cur_dir)
      end
    end
    def store_directory(dir)
      tmp = Tempfile.open("s3backup")
      tmp.close
      #tgzのファイルをtmp.pathに作成
      to_tgz(tmp.path,dir)
      #S3にディレクトリの絶対パスをキーにして、圧縮したデータをストア
      i=1
      key = nil
      if @aes
        key = @aes.encrypt(dir)
      else
        key = dir
      end
      #前回のバックアップデータ削除
      cnt = 1
      while @target.exists?(cnt.to_s() + "_" + key)
        @target.delete(cnt.to_s() + "_" + key)
        cnt+=1
      end
      f = File.open(tmp.path,"r")
      begin
        while 1
          key_name = i.to_s()+"_"+key
          data = f.readpartial(@buf_size)
          if @aes
            data = @aes.encrypt(data)
          end
          @target.post(key_name,data)
          i+=1
        end
      rescue EOFError
      end
      tmp.close(true)
    end
    def delete_direcory(dir)
      if @aes
        dir = @aes.encrypt(dir)
      end
      i=1
      while @target.delete("#{i}_#{dir}")
        i+=1
      end
    end
    def differential_copy(dir)
      #現在のファイル・ツリーを比較
      tree_info = TreeInfo.new(dir)

      target_tree_name = "tree_"+dir+".yml"
      tree_data = nil
      #前回のファイル・ツリーを取得
      old_tree = TreeInfo.new(@target.get(target_tree_name))

      #前回と今回のファイル・ツリーを比較
      diff_info = tree_info.diff(old_tree)
      S3log.debug("diff_info=#{diff_info.inspect}")

      update_dir = diff_info[:directory][:add] + diff_info[:directory][:modify]
      #更新されたディレクトリをアップロード
      update_dir.each do |udir|
        store_directory(udir)
        udir_info = tree_info.get_dir_info(udir)
        #前回のファイル・ツリー情報のうち、今回アップデートしたディレクトリ情報ファイル情報を更新
        old_tree.update_dir(udir,udir_info)
        #更新したファイル・ツリー情報をアップロード(途中で失敗しても、resumeできるようにするため。)
        @target.post(target_tree_name,old_tree.dump_yaml)
      end
      diff_info[:directory][:remove].each do |rm_dir|
        delete_direcory(rm_dir)
      end
      #今回のファイル・ツリーをAWS S3に登録
      @target.post(target_tree_name,tree_info.dump_yaml)
    end
    def get_target_tree(dir)
      base_dir = dir
      tree_data = nil
      before_base=""
      #バックアップしたディレクトリよりも下位のディレクトリが指定されることがあるため
      while 1
        base = base_dir
        if base == before_base
          break
        end
        tree_file_name = "tree_"+base+".yml"
        tree_data = @target.get(tree_file_name)
        if tree_data
          break
        end
        before_base = base
        base_dir = File.dirname(base_dir)
      end
      unless tree_data
        return nil
      end
      return TreeInfo.new(tree_data)
    end
    def get_target_bases
      files = @target.find(/^tree_.*\.yml/)
      dirs = files.map do |d| 
        m=/tree_(.*)\.yml/.match(d)
        next nil unless m
        m[1]
      end
      return dirs.compact
    end
    def expand_tree(dir,tree_info,output_dir)
      tree = tree_info.hierarchie(dir)
      top = tree[0].keys[0]
      top_dir = File.dirname(top)
      tmp_dir = CGI.escape(top_dir)
      output_dir = output_dir+"/"+tmp_dir
      FileUtils.mkdir_p(output_dir)
      tree.each do |node|
        get_directories(node.keys,top_dir,output_dir)
      end
      top_dir_len = top_dir.length
      (tree.length - 1).downto(0){|n|
        tree[n].each do |k,v|
          dir_len = k.length
          relative_path = k.slice(top_dir_len,dir_len - top_dir_len)
          dir = output_dir + relative_path
          File.utime(v[:atime],v[:mtime],dir)
        end
      }
    end
    def restore(dir,output_dir)
      tree = get_target_tree(dir)
      unless tree
        S3log.warn("#{dir} isn't find in AWS S3. ignore")
        return
      end
      expand_tree(dir,tree,output_dir)
      S3log.debug("expand_tree=#{tree.inspect}")
    end
  end
end
