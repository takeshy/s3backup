require 'yaml'
require 'sqlite3'
module S3backup
  class TreeInfo
    attr_reader :db_name,:db
    def make_table
      sql = "create table directory ( id INTEGER PRIMARY KEY ,name varchar(2048), mtime integer, parent_directory_id integer)"
      @db.execute(sql)
      sql = "create table file ( name varchar(2048), size  integer, mtime integer,directory_id integer)"
      @db.execute(sql)
      sql = "create table symlink ( name varchar(2048), source varchar(2048),directory_id integer)"
      @db.execute(sql)
      sql = "CREATE INDEX idx_directory_name ON directory(name)"
      @db.execute(sql)
      sql = "CREATE INDEX idx_directory_parent_directory_id ON directory(parent_directory_id)"
      @db.execute(sql)
      sql = "CREATE INDEX idx_file_directory_id ON file(directory_id)"
      @db.execute(sql)
      sql = "CREATE INDEX idx_file_name  ON file(name)"
      @db.execute(sql)
      sql = "CREATE INDEX idx_symlink_name  ON symlink(name)"
      @db.execute(sql)
      sql = "CREATE INDEX idx_symlink_directory_id  ON symlink(directory_id)"
      @db.execute(sql)
    end
    def check_dirs(p_id,p_name)
      @db.execute('select id,name from directory where parent_directory_id = ?',p_id) do |row|
        id = row[0]
        name = row[1]
        if File.basename(name) == File.basename(p_name)
          sql = "insert into file(name,size,mtime,directory_id) values (:name, :size, :mtime,:directory_id)"
          @db.execute(sql,:name=>"absolutely_no_exist_file_name#{id}",:size=>0,:mtime =>0,:directory_id=>p_id)
        end
        check_dirs(id,name)
      end
    end
    def convert_yaml_to_sqlite3(file_map)
      file_map[:directory].keys().sort{|a,b| a<=>b}.each do |key|
        file_at = file_map[:directory][key]
        sql = "insert into directory(name,mtime) values (:name, :mtime)"
        @db.execute(sql,:name=>key,:mtime => file_at[:mtime].to_i )
      end
      @db.execute('select id,name from directory' ) do |row|
        dir_id = row[0].to_i
        parent = File.dirname(row[1])
        @db.execute('select id from directory where name =?',parent ) do |row|
          @db.execute("update directory set parent_directory_id = #{row[0]} where id = #{dir_id}")
        end
      end
      #for bug (same name directory was not backuped before)
      @db.execute('select id,name from directory order by id limit 1') do |row|
        p_id = row[0]
        name = row[1]
        check_dirs(p_id,name)
      end
      file_map[:file].each do |key,val|
        file_at = file_map[:file][key]
        dir_name = File.dirname(key)
        dir_id = nil
        @db.execute('select id from directory where name=?',dir_name ) do |row|
          #rowは結果の配列
          dir_id = row[0].to_i
        end
        unless dir_id
          STDERR.print "directory name isn't exist ignore #{dir_name}"
          next
        end
        sql = "insert into file(name,size,mtime,directory_id) values (:name, :size, :mtime,:directory_id)"
        @db.execute(sql,:name=>key,:size=>file_at[:size],:mtime => file_at[:date].to_i, :directory_id=>dir_id)
      end
      file_map[:symlink].each do |key,val|
        file_at = file_map[:symlink][key]
        dir_name = File.dirname(key)
        sql="select id from directory where name = :name"
        dir_id = nil
        @db.execute('select id from directory where name=?',dir_name ) do |row|
          #rowは結果の配列
          dir_id = row[0].to_i
        end
        unless dir_id
          STDERR.print "directory name isn't exist ignore #{dir_name}"
          next
        end
        sql = "insert into symlink(name,source,directory_id) values (:name, :source,:directory_id)"
        @db.execute(sql,:name=>key,:source=>file_at[:source],:directory_id=>dir_id)
      end
    end
    def initialize(opt)
      @db_name = opt[:db]
      @db = SQLite3::Database.new(opt[:db])
      if opt[:format].nil?
        make_table
      elsif opt[:format] == :directory
        make_table
        stat = File.stat(opt[:directory])
        sql = "insert into directory(name,mtime) values (:name, :mtime)"
        @db.execute(sql,:name=>opt[:directory],:mtime =>stat.mtime.to_i)
        dir_id = nil
        @db.execute('select id from directory where name=?',opt[:directory]) do |row|
          #rowは結果の配列
          dir_id = row[0].to_i
        end
        makeFileMap(opt[:directory],dir_id)
      elsif opt[:format] == :yaml
        make_table
        convert_yaml_to_sqlite3(YAML.load(opt[:data]))
      end
    end
    def makeFileMap(dir,id)
      Dir.entries(dir).each do |e|
        if e == "." or e == ".."
          next
        end
        name = dir + "/" + e
        if File.directory?(name)
          stat = File.stat(name)
          sql = "insert into directory(name,mtime,parent_directory_id) values (:name, :mtime,:parent_directory_id)"
          @db.execute(sql,:name=>name,:mtime =>stat.mtime.to_i,:parent_directory_id=>id)
          dir_id = nil
          @db.execute('select id from directory where name=?',name) do |row|
            #rowは結果の配列
            dir_id = row[0].to_i
          end
          makeFileMap(name,dir_id)
        elsif File.symlink?(name)
          sql = "insert into symlink(name,source,directory_id) values (:name, :source,:directory_id)"
          @db.execute(sql,:name=>name,:source=>File.readlink(name),:directory_id=>id)
        else
          stat = File.stat(name)
          sql = "insert into file(name,size,mtime,directory_id) values (:name, :size, :mtime,:directory_id)"
          @db.execute(sql,:name=>name,:size=>stat.size,:mtime => stat.mtime.to_i, :directory_id=>id)
        end
      end
    end
    def update_dir(dir_info)
      result = @db.execute("select id from directory where name = ?",dir_info[:name])
      p_id = nil
      id = nil
      @db.execute('select id from directory where name =?',File.dirname(dir_info[:name])) do |row|
        p_id = row[0]
      end
      if result.length != 0
        id = result[0][0]
        @db.execute("delete from file where directory_id = #{id}")
        @db.execute("delete from symlink where directory_id = #{id}")

        @db.execute("update directory  set mtime = ?,parent_directory_id = ?" + 
                    " where id = ?",dir_info[:mtime].to_i,p_id,id)
      else
        @db.execute("insert into directory(name,mtime,parent_directory_id) values(?,?,?)",
          dir_info[:name],dir_info[:mtime].to_i,p_id)
        result = @db.execute("select id from directory where name = ?",dir_info[:name])
        id = result[0][0]
      end
      dir_info[:files].each do |f|
        @db.execute("insert into file(name,mtime,size,directory_id) values(?,?,?,?)",
                    f[:name],f[:mtime].to_i,f[:size],id)
      end
      dir_info[:links].each do |f|
        @db.execute("insert into symlink(name,source,directory_id) values(?,?,?)",f[:name],f[:source],id)
      end
    end
    def get_level_directory(tree,p_id,level)
      @db.execute('select id,name,mtime from directory where parent_directory_id = ?',p_id) do |row|
        id = row[0]
        name = row[1]
        mtime = row[2].to_i
        tree[level] = {} unless tree[level]
        tree[level][name] = {:mtime=>mtime.to_i}
        get_level_directory(tree,id,level+1)
      end
    end
    def hierarchie(dir)
      tree=[]
      result = @db.execute('select id,name,mtime from directory where name = ?',dir)
      if result.length == 0
        S3log.error("#{dir} is not stored.")
        exit(-1)
      end
      id = result[0][0]
      name = result[0][1]
      mtime = result[0][2].to_i
      tree[0] = {} 
      tree[0][name]={:mtime=>mtime}
      get_level_directory(tree,id,1)
      return tree
    end
    def modify(target)
      now_id = 0
      while 1
        result = @db.execute("select id,name,mtime from directory where id > ? limit 1",now_id)
        break if result.length == 0
        now_id = result[0][0].to_i
        name = result[0][1]
        mtime = result[0][2].to_i
        files = []
        links = []
        t_result = target.db.execute("select id,name from directory where name = ?",name)
        if t_result.length != 0
          t_id = t_result[0][0]
          t_files = target.db.execute("select name,size,mtime from file where directory_id = ? order by name",t_id)
          files = @db.execute("select name,size,mtime from file where directory_id = ? order by name",now_id)
          if t_files == files
            t_links = target.db.execute("select name,source from symlink where directory_id = ? order by name",t_id)
            links = @db.execute("select name,source from symlink where directory_id = ? order by name",now_id)
            if t_links == links
              next
            end
          end
        end
        file_infos = []
        files.each do |f|
          file_infos.push({:name=>f[0],:size=>f[1],:mtime=>f[2].to_i})
        end
        sym_infos = []
        links.each do |l|
          sym_infos.push({:name=>l[0],:source=>l[1]})
        end
        yield({:name => name,:mtime=>mtime.to_i,:files => file_infos ,:links => sym_infos})
      end
    end
    def remove(target)
      now_id = 0
      while 1
        t_result = target.db.execute("select id,name from directory where id > ? limit 1",now_id)
        break if t_result.length == 0
        now_id = t_result[0][0].to_i
        name = t_result[0][1]
        result = @db.execute("select id,name from directory where name = ?",name)
        if result.length == 0
          yield({:name => name})
        end
      end
    end
    def close(delete=false)
      @db.close
      if delete
        if File.exist?(@db_name)
          File.unlink(@db_name)
        end
        if File.exist?(@db_name + ".gz")
          File.unlink(@db_name+".gz")
        end
      end
    end
  end
end
