GitHub GitHub

    Explore
    Gist
    Blog
    Help

julianpeeters

    1
    52

public johnynek / scalding forked from twitter/scalding

    Code
    Network
    Pull Requests 0
    Wiki
    Graphs

    Tags
    Downloads

    Files
    Commits
    Branches 23

scalding / scripts / scald.rb
johnynek 2 days ago
Fixes for scald.rb on linux/ruby 1.9

4 contributors
executable file 432 lines (378 sloc) 13.994 kb

1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
53
54
55
56
57
58
59
60
61
62
63
64
65
66
67
68
69
70
71
72
73
74
75
76
77
78
79
80
81
82
83
84
85
86
87
88
89
90
91
92
93
94
95
96
97
98
99
100
101
102
103
104
105
106
107
108
109
110
111
112
113
114
115
116
117
118
119
120
121
122
123
124
125
126
127
128
129
130
131
132
133
134
135
136
137
138
139
140
141
142
143
144
145
146
147
148
149
150
151
152
153
154
155
156
157
158
159
160
161
162
163
164
165
166
167
168
169
170
171
172
173
174
175
176
177
178
179
180
181
182
183
184
185
186
187
188
189
190
191
192
193
194
195
196
197
198
199
200
201
202
203
204
205
206
207
208
209
210
211
212
213
214
215
216
217
218
219
220
221
222
223
224
225
226
227
228
229
230
231
232
233
234
235
236
237
238
239
240
241
242
243
244
245
246
247
248
249
250
251
252
253
254
255
256
257
258
259
260
261
262
263
264
265
266
267
268
269
270
271
272
273
274
275
276
277
278
279
280
281
282
283
284
285
286
287
288
289
290
291
292
293
294
295
296
297
298
299
300
301
302
303
304
305
306
307
308
309
310
311
312
313
314
315
316
317
318
319
320
321
322
323
324
325
326
327
328
329
330
331
332
333
334
335
336
337
338
339
340
341
342
343
344
345
346
347
348
349
350
351
352
353
354
355
356
357
358
359
360
361
362
363
364
365
366
367
368
369
370
371
372
373
374
375
376
377
378
379
380
381
382
383
384
385
386
387
388
389
390
391
392
393
394
395
396
397
398
399
400
401
402
403
404
405
406
407
408
409
410
411
412
413
414
415
416
417
418
419
420
421
422
423
424
425
426
427
428
429
430
431

	

#!/usr/bin/env ruby
$LOAD_PATH << File.join(File.expand_path(File.dirname(File.symlink?(__FILE__) ? File.readlink(__FILE__) : __FILE__)), 'lib')

require 'fileutils'
require 'open-uri'
require 'thread'
require 'trollop'
require 'yaml'

SCALDING_VERSION="0.7.3"

USAGE = <<END
Usage : scald.rb [--jar jarfile] [--hdfs|--hdfs-local|--local|--print] job <job args>
--clean: clean rsync and maven state before running
--jar ads-batch: specify the jar file
--hdfs: if job ends in ".scala" or ".java" and the file exists, link it against JARFILE (below) and then run it on HOST.
else, it is assumed to be a full classname to an item in the JARFILE, which is run on HOST
--hdfs-local: run in hadoop local mode (--local is cascading local mode)
--host: specify the hadoop host where the job runs
--local: run in cascading local mode (does not use hadoop)
--print: print the command YOU SHOULD ENTER on the remote node. Useful for screen sessions.
END

##############################################################
# Default configuration:
  #Get the absolute path of the original (non-symlink) file.
CONFIG_DEFAULT = begin
  original_file = File.symlink?(__FILE__) ? File.readlink(__FILE__) : __FILE__
  repo_root = File.expand_path(File.dirname(original_file)+"/../")
  { "host" => "my.host.here", #where the job is rsynced to and run
    "repo_root" => repo_root, #full path to the repo you use, Twitter specific
    "jar" => repo_root + "/target/scalding-assembly-#{SCALDING_VERSION}.jar", #what jar has all the depencies for this job
    "localmem" => "3g", #how much memory for java to use when running in local mode
    "namespaces" => { "abj" => "com.twitter.ads.batch.job", "s" => "com.twitter.scalding" },
    "hadoop_opts" => { "mapred.reduce.tasks" => 20, #be conservative by default
                       "mapred.min.split.size" => "2000000000" }, #2 billion bytes!!!
    "depends" => [ "org.apache.hadoop/hadoop-core/0.20.2",
                   "log4j/log4j/1.2.15",
                   "commons-httpclient/commons-httpclient/3.1",
                   "commons-cli/commons-cli/1.2",
                   "org.apache.zookeeper/zookeeper/3.3.4" ],
    "default_mode" => "--hdfs"
  }
end
##############################################################

CONFIG_RC = begin
#put configuration in .scaldrc in HOME to override the defaults below:
    YAML.load_file(ENV['HOME'] + "/.scaldrc") || {} #seems that on ruby 1.9, this returns false on failure
  rescue
    {}
  end

CONFIG = CONFIG_DEFAULT.merge!(CONFIG_RC)

#optionally set variables (not linux often doesn't have this set, and falls back to TMP. Set up a
#YAML file in .scaldrc with "tmpdir: my_tmp_directory_name" or export TMPDIR="/my/tmp" to set on
#linux
TMPDIR=CONFIG["tmpdir"] || ENV['TMPDIR'] || "/tmp"
TMPMAVENDIR = File.join(TMPDIR, "maven")
BUILDDIR=CONFIG["builddir"] || File.join(TMPDIR,"script-build")
LOCALMEM=CONFIG["localmem"] || "3g"
DEPENDENCIES=CONFIG["depends"] || []
RSYNC_STATFILE_PREFIX = TMPDIR + "/scald.touch."

#Recall that usage is of the form scald.rb [--jar jarfile] [--hdfs|--hdfs-local|--local|--print] job <job args>
#This parser holds the {job <job args>} part of the command.
OPTS_PARSER = Trollop::Parser.new do
  opt :clean, "Clean all rsync and maven state before running"
  opt :hdfs, "Run on HDFS"
  opt :hdfs_local, "Run in Hadoop local mode"
  opt :local, "Run in Cascading local mode (does not use Hadoop)"
  opt :print, "Print the command YOU SHOULD enter on the remote node. Useful for screen sessions"

  opt :jar, "Specify the jar file", :type => String
  opt :host, "Specify the hadoop host where the job runs", :type => String
  opt :reducers, "Specify the number of reducers", :type => :int

  stop_on_unknown #Stop parsing for options parameters once we reach the job file.
end

#OPTS holds the option parameters that come before {job}, i.e., the
#[--jar jarfile] [--hdfs|--hdfs-local|--local|--print] part of the command.
OPTS = OPTS_PARSER.parse ARGV

#Make sure one of the execution modes is set.
unless [OPTS[:hdfs], OPTS[:hdfs_local], OPTS[:local], OPTS[:print]].any?
  #Modes in CONFIG file are in the form "--hdfs" or "--local", but the OPTS hash assumes
  #them to be in the form :hdfs or :local.
  #TODO: Add error checking on CONFIG["default_mode"]?
  mode = CONFIG["default_mode"] ? CONFIG["default_mode"].gsub("--", "").to_sym : :hdfs
  OPTS[mode] = true
end

def maven_filename(jar_filename)
  File.join(TMPMAVENDIR, jar_filename)
end

if OPTS[:clean]
  rsync_files = Dir.glob(RSYNC_STATFILE_PREFIX + "*")
  $stderr.puts("Cleaning rsync stat files: #{rsync_files.join(', ')}")
  rsync_files.each { |f| File.delete(f) }

  maven_files = Dir.glob(maven_filename("*"))
  $stderr.puts("Cleaning maven jars: #{maven_files.join(', ')}")
  maven_files.each { |f| File.delete(f) }
  Dir.rmdir(TMPMAVENDIR) if File.directory?(TMPMAVENDIR)
  #HACK -- exit immediately because other parts of this script assume more
  #arguments are passed in.
  exit(0)
end

if ARGV.size < 1
  $stderr.puts USAGE
  Trollop::options
  Trollop::die "insufficient arguments passed to scald.rb"
end

SBT_HOME="#{ENV['HOME']}/.sbt"
COMPILE_CMD="java -cp #{SBT_HOME}/boot/scala-2.8.1/lib/scala-library.jar:#{SBT_HOME}/boot/scala-2.8.1/lib/scala-compiler.jar -Dscala.home=#{SBT_HOME}/boot/scala-2.8.1/lib/ scala.tools.nsc.Main"

HOST = OPTS[:host] || CONFIG["host"]

JARFILE =
  if OPTS[:jar]
    jarname = OPTS[:jar]
    #highly Twitter specific here:
    CONFIG["repo_root"] + "/dist/#{jarname}-deploy.jar"
  else
    CONFIG["jar"]
  end

JOBFILE=OPTS_PARSER.leftovers.first
JOB_ARGS=OPTS_PARSER.leftovers[1..-1].join(" ")

#Check that we have all the dependencies, and download any we don't.
def maven_get(dependencies = DEPENDENCIES)
  #First, make sure the TMPMAVENDIR exists and create it if not.
  FileUtils.mkdir_p(TMPMAVENDIR)

  #Now make sure all the dependencies exist.
  dependencies.each do |dependency|
    jar_filename = dependency_to_jar(dependency)

    #Check if we already have the jar. Get it if not.
    if !File.exists?(maven_filename(jar_filename))
      url = dependency_to_url(dependency)
      uri = URI(dependency_to_url(dependency))
      $stderr.puts("downloading #{jar_filename} from #{uri}...")

      File.open(maven_filename(jar_filename), "wb") do |f|
        begin
          f.print open(url).read
          $stderr.puts "Successfully downloaded #{jar_filename}!"
        rescue SocketError => e
          $stderr.puts "SocketError in downloading #{jar_filename}: #{e}"
        rescue SystemCallError => e
          $stderr.puts "SystemCallError in downloading #{jar_filename}: #{e}"
        end
      end
    end
  end
end

#Converts an array of dependencies into an array of jar filenames.
#Example:
#Input dependencies: ["org.apache.hadoop/hadoop-core/0.20.0", "com.twitter/scalding/0.2.0"]
#Output jar filenames: ["/tmp/mvn/hadoop-core-0.20.0.jar","/tmp/mvn/scalding-0.2.0.jar"]
def convert_dependencies_to_jars(dependencies = DEPENDENCIES, as_classpath = false)
  ret = dependencies.map{ |d| maven_filename(dependency_to_jar(d)) }
  ret = ret.join(":") if as_classpath
  ret
end

#Convert a maven dependency to a url for downloading.
#Example:
#Input dependency: org.apache.hadoop/hadoop-core/0.20.2
#Output url: http://repo1.maven.org/maven2/org/apache/hadoop/hadoop-core/0.20.2/hadoop-core-0.20.2.jar
def dependency_to_url(dependency)
  #Each dependency is in the form group/artifact/version.
  group, artifact, version = dependency.split("/")
  jar_filename = dependency_to_jar(dependency)
  group_with_slash = group.split(".").join("/")
  "http://repo1.maven.org/maven2/#{group_with_slash}/#{artifact}/#{version}/#{jar_filename}"
end

#Convert a maven dependency to the name of its jar file.
#Example:
#Input dependency: org.apache.hadoop/hadoop-core/0.20.2
#Output: hadoop-core-0.20.2.jar
def dependency_to_jar(dependency)
  group, artifact, version = dependency.split("/")
  "#{artifact}-#{version}.jar"
end

def hadoop_opts
  opts = CONFIG["hadoop_opts"] || {}
  opts["mapred.reduce.tasks"] = OPTS[:reducers] if OPTS[:reducers]
  if !opts.has_key?("mapred.reduce.tasks")
    Trollop::die "number of reducers not set"
  end
  opts.collect { |k,v| "-D#{k}=#{v}" }.join(" ")
end

def file_type
  JOBFILE =~ /\.(scala|java)$/
  $1
end

def is_file?
  !file_type.nil?
end

EXTENSION_RE = /(.*)\.(scala|java)$/

#Get the name of the job from the file.
#the rule is: last class in the file, or the one that matches the filename
def get_job_name(file)
  package = ""
  job = nil
  default = nil
  if file =~ EXTENSION_RE
    default = $1
    File.readlines(file).each { |s|
      if s =~ /^package ([^;]+)/
        package = $1.chop + "."
      elsif s =~ /class\s+([^\s(]+).*extends\s+.*Job/
        unless job and default and (job.downcase == default.downcase)
          #use either the last class, or the one with the same name as the file
          job = $1
        end
      end
    }
    raise "Could not find job name" unless job
    "#{package}#{job}"
  elsif file =~ /(.*):(.*)/
    begin
      CONFIG["namespaces"][$1] + "." + $2
    rescue
      $stderr.puts "Unknown namespace: #{$1}"
      exit(1)
    end
  else
    file
  end
end

JARPATH=File.expand_path(JARFILE)
JARBASE=File.basename(JARFILE)
JOBPATH=File.expand_path(JOBFILE)
JOB=get_job_name(JOBFILE)
JOBJAR=JOB+".jar"
JOBJARPATH=TMPDIR+"/"+JOBJAR


class ThreadList
  def initialize
    @threads = []
    @failures = []
    @mtx = Mutex.new
    @open = true
  end

  def thread(*targs, &block)
    @mtx.synchronize {
      if @open
        @threads << Thread.new(*targs, &block)
      else
        raise "ThreadList is closed"
      end
    }
  end

  def failure(f)
    @mtx.synchronize {
      @failures << f
    }
  end

  #if the size > 0, execute a block then join, else no not yield.
  #returns thread count
  def waitall
    block = false
    size = @mtx.synchronize {
      if @open
        @open = false
        block = true
      end
      @threads.size
    }
    if block
      yield size
      @threads.each { |t| t.join }
      #at this point, all threads are finished.
      if @failures.each { |f| $stderr.puts f }.size > 0
        raise "There were failures"
      end
    end
    size
  end
end

THREADS = ThreadList.new

# Returns file size formatted with a human-readable suffix.
def human_filesize(filename)
  size = File.size(filename)
  case
  when size < 2**10: '%d bytes' % size
  when size < 2**20: '%.1fK' % (1.0 * size / 2**10)
  when size < 2**30: '%.1fM' % (1.0 * size / 2**20)
  else '%.1fG' % (1.0 * size / 2**30)
  end
end

#this is used to record the last time we rsynced
def rsync_stat_file(filename)
  RSYNC_STATFILE_PREFIX+filename.gsub(/\//,'.')+"."+HOST
end

#In another thread, rsync the file. If it succeeds, touch the rsync_stat_file
def rsync(from, to)
  rtouch = rsync_stat_file(from)
  if !File.exists?(rtouch) || File.stat(rtouch).mtime < File.stat(from).mtime
    $stderr.puts("rsyncing #{human_filesize(from)} from #{to} to #{HOST} in background...")
    THREADS.thread(from, to) { |ff,tt|
      if system("rsync -e ssh -z #{ff} #{HOST}:#{tt}")
        #this indicates success and notes the time
        FileUtils.touch(rtouch)
      else
        #indicate failure
        THREADS.failure("Could not rsync: #{ff} to #{HOST}:#{tt}")
        FileUtils.rm_f(rtouch)
      end
    }
  end
end

def is_local?
  OPTS[:local] || OPTS[:hdfs_local]
end

def needs_rebuild?
  if !File.exists?(JOBJARPATH)
    true
  else
    #the jar exists, but is it fresh enough:
    mtime = File.stat(JOBJARPATH).mtime
    [ # if the jar is older than the path of the job file:
      (mtime < File.stat(JOBPATH).mtime),
      # if the jobjar is older than the main jar:
      mtime < File.stat(JARPATH).mtime ].any?
  end
end

def build_job_jar
  $stderr.puts("compiling " + JOBFILE)
  FileUtils.mkdir_p(BUILDDIR)
  classpath = (convert_dependencies_to_jars + [JARPATH]).join(":")
  puts("#{file_type}c -classpath #{classpath} -d #{BUILDDIR} #{JOBFILE}")
  unless system("#{COMPILE_CMD} -classpath #{JARPATH} -d #{BUILDDIR} #{JOBFILE}")
    FileUtils.rm_f(rsync_stat_file(JOBJARPATH))
    FileUtils.rm_rf(BUILDDIR)
    exit(1)
  end

  FileUtils.rm_f(JOBJARPATH)
  system("jar cf #{JOBJARPATH} -C #{BUILDDIR} .")
  FileUtils.rm_rf(BUILDDIR)
end

def hadoop_command
  "HADOOP_CLASSPATH=/usr/share/java/hadoop-lzo-0.4.15.jar:#{JARBASE}:job-jars/#{JOBJAR} " +
    "hadoop jar #{JARBASE} -libjars job-jars/#{JOBJAR} #{hadoop_opts} #{JOB} --hdfs " +
    JOB_ARGS
end

def jar_mode_command
  "hadoop jar #{JARBASE} #{hadoop_opts} #{JOB} --hdfs " + JOB_ARGS
end

#Always sync the remote JARFILE
rsync(JARPATH, JARBASE) if !is_local?
#make sure we have the dependencies to compile and run locally (these are not in the above jar)
#this does nothing if we already have the deps.
maven_get
if is_file?
  build_job_jar if needs_rebuild?

  if !is_local?
    #Make sure the job-jars/ directory exists before rsyncing to it
    system("ssh #{HOST} '[ ! -d job-jars/ ] && mkdir job-jars/'")
    #rsync only acts if the file is out of date
    rsync(JOBJARPATH, "job-jars/" + JOBJAR)
  end
end

def local_cmd(mode)
  classpath = (convert_dependencies_to_jars + [JARPATH]).join(":") + (is_file? ? ":#{JOBJARPATH}" : "")
  "java -Xmx#{LOCALMEM} -cp #{classpath} com.twitter.scalding.Tool #{JOB} #{mode} " + JOB_ARGS
end

SHELL_COMMAND =
  if OPTS[:hdfs]
    if is_file?
      "ssh -t -C #{HOST} #{hadoop_command}"
    else
      "ssh -t -C #{HOST} #{jar_mode_command}"
    end
  elsif OPTS[:hdfs_local]
    local_cmd("--hdfs")
  elsif OPTS[:local]
    local_cmd("--local")
  elsif OPTS[:print]
    if is_file?
      "echo #{hadoop_command}"
    else
      "echo #{jar_mode_command}"
    end
  else
    Trollop::die "no mode set"
  end

#Now block on all the threads:
begin
  THREADS.waitall { |c| puts "Waiting for #{c} background thread#{c > 1 ? 's' : ''}..." if c > 0 }
  #If there are no errors:
  exit(system(SHELL_COMMAND))
rescue
  exit(1)
end

GitHub Links

    GitHub
    About
    Blog
    Features
    Contact & Support
    Training
    GitHub Enterprise
    Site Status

    Clients
    GitHub for Mac
    GitHub for Windows
    GitHub for Eclipse
    GitHub Mobile Apps

    Tools
    Gauges: Web analytics
    Speaker Deck: Presentations
    Gist: Code snippets
    Extras
    Job Board
    GitHub Shop
    The Octodex

    Documentation
    GitHub Help
    Developer API
    GitHub Flavored Markdown
    GitHub Pages

    Terms of Service
    Privacy
    Security

© 2012 GitHub Inc. All rights reserved.
Dedicated Server Powered by the Dedicated Servers and
Cloud Computing of Rackspace Hosting®

