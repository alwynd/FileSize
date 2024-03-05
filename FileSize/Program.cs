using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Common namespace
/// </summary>
namespace FileSize
{

    /// <summary>
    /// Main Program
    /// </summary>
    public class Program
    {

        public static bool VERBOSE = false;

        /// <summary>
        /// Main entry point.
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args)
        {
            long time = NBConsole.Stamp();
            try
            {
                NBConsole.Log($"FileSize.Program.Main:-- START, Arguments: [folder]");

                string folder = args[0];
                FileSizeCalclator calc = new FileSizeCalclator();
                calc.CalculateFileSize(folder);

            } //try
            finally
            {
                NBConsole.Log($"FileSize.Program.Main:--  DONE, took: {NBConsole.Stamp(time)}");
                while (!NBConsole.Done()) { }
            } //finally
            
            
        }
    }

    /// <summary>
    /// Console logging.
    /// </summary>
    public static class NBConsole
    {
        /// <summary>
        /// Collection.
        /// </summary>
        private static readonly ConcurrentQueue<string> QUEUE = new ConcurrentQueue<string>();

        /// <summary>
        /// Constructor.
        /// </summary>
        static NBConsole()
        {
            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    if (QUEUE.Count > 0)
                    {
                        string msg = null;
                        QUEUE.TryDequeue(out msg);

                        if (msg != null)
                        {
                            Console.WriteLine(msg);
                        } //if
                    } //if
                }
            });
        }

        /// <summary>
        /// Done?
        /// </summary>
        /// <returns></returns>
        public static bool Done()
        {
            return QUEUE.Count < 1;
        }


        /// <summary>
        /// Log to console.
        /// </summary>
        /// <param name="msg"></param>
        public static void Log(string msg)
        {
            QUEUE.Enqueue($"{DateTime.UtcNow.ToString()} - [DEBUG] {msg}");
        }

        /// <summary>
        /// Stamp now.
        /// </summary>
        /// <returns></returns>
        public static long Stamp()
        {
            return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }

        /// <summary>
        /// Stamps from time.
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public static string Stamp(long time)
        {
            return TimeSpan.FromMilliseconds(Stamp() - time).ToString();
        }
    }

    /// <summary>
    /// File Holder.
    /// </summary>
    public class FileHolder
    {
        public string Name { get; set; }
        public long Size { get; set; }

        /// <summary>
        /// Implement tostring
        /// </summary>
        public override string ToString()
        {
            return $"{FileSizeCalclator.FormatFileSize(Size), 16} - {Name}";
        }
    }

    /// <summary>
    /// Calculates file size.
    /// </summary>
    public class FileSizeCalclator
    {

        private static string[] sizes = { "B", "KB", "MB", "GB", "TB" };

        /// <summary>
        /// The file size map.
        /// </summary>
        public ConcurrentDictionary<string, long> FileSizeMap { get; private set; } = new ConcurrentDictionary<string, long>();

        /// <summary>
        /// Calculates File Size.
        /// </summary>
        public void CalculateFileSize(string folder)
        {
            long time = NBConsole.Stamp();
            try
            {
                NBConsole.Log($"FileSizeCalclator.CalculateFileSize:-- START, folder: {folder}");

                ConcurrentQueue<FileHolder> allfiles = new ConcurrentQueue<FileHolder>();
                ConcurrentQueue<string> folders = new ConcurrentQueue<string>();

                BatchAndSplitTopLevel(folder, allfiles, folders);
                BatchAndProcessAllFiles(folders, allfiles);

                List<FileHolder> allFilesList = FlattenList(allfiles);
                ProcessFileSizeMap(allFilesList);

                // display...
                List<string> keys = new List<string>(FileSizeMap.Keys);
                keys.Sort();
                keys.ForEach(x => NBConsole.Log($"FileSizeCalclator.CalculateFileSize file: {FormatFileSize(FileSizeMap[x]), 16} - {x}"));

            } //try
            finally
            {
                NBConsole.Log($"FileSizeCalclator.CalculateFileSize:--  DONE, took: {NBConsole.Stamp(time)}");
                while (!NBConsole.Done()) { }
            } //finally

        }


        /// <summary>
        /// Process and build the FileSizeMap from each file, and group tally up to the parent folders.
        /// </summary>
        /// <param name="allfiles"></param>
        public void ProcessFileSizeMap(List<FileHolder> allfiles)
        {
            long time = NBConsole.Stamp();
            NBConsole.Log($"FileSizeCalclator.ProcessFileSizeMap:-- START, allFiles: {allfiles.Count}");

            Parallel.ForEach(Partitioner.Create(0, allfiles.Count, 64), range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    ProcessFileSizeMap(allfiles[i]);
                } //for
            }); //Parallel


            NBConsole.Log($"FileSizeCalclator.ProcessFileSizeMap:--  DONE, allFiles: {allfiles.Count}, took: {NBConsole.Stamp(time)}");
        }

        /// <summary>
        /// Process single file to map.
        /// </summary>
        private void ProcessFileSizeMap(FileHolder fileHolder)
        {
            // first assume unix. (works on windows too)
            string[] paths = fileHolder.Name.Replace("\\", "/").Split('/');

            // group keys
            List<string> keys = new List<string>();
            string key = "";
            Array.ForEach(paths, x => { key += ("/" + x); keys.Add(key); });

            Parallel.ForEach(keys, x=> 
            {
                FileSizeMap.AddOrUpdate(x, fileHolder.Size, (id, count) => count + fileHolder.Size);
            });

            if (Program.VERBOSE) keys.ForEach(x => NBConsole.Log($"FileSizeCalclator.ProcessFileSizeMap key: {x}"));
        }

        /// <summary>
        /// Group and tally folder sizes.
        /// </summary>
        private List<FileHolder> FlattenList(ConcurrentQueue<FileHolder> allfiles)
        {
            List<FileHolder> allFilesList = new List<FileHolder>();
            while (allfiles.Count > 0)
            {
                FileHolder fld = null;
                allfiles.TryDequeue(out fld);
                if (fld != null) allFilesList.Add(fld);
            } //while

            // sort
            allFilesList = allFilesList.OrderBy(o => o.Name).ToList();
            if (Program.VERBOSE) allFilesList.ForEach(x => NBConsole.Log($"FileSizeCalclator.FlattenList file: {x}"));
            return allFilesList;
        }

        /// <summary>
        /// Batch and process all files.
        /// </summary>
        private void BatchAndProcessAllFiles(ConcurrentQueue<string> folders, ConcurrentQueue<FileHolder> allfiles)
        {
            NBConsole.Log($"FileSizeCalclator.BatchAndProcessAllFiles top level folders: folders.Count : {folders.Count }");
            if (folders.Count < 1) return;
            List<string> foldersList = new List<string>();
            while (folders.Count > 0)
            {
                string fld = null;
                folders.TryDequeue(out fld);
                if (fld != null) foldersList.Add(fld);
                NBConsole.Log($"FileSizeCalclator.BatchAndProcessAllFiles top level folders: folder: {fld}");
            } //while
            foldersList.Sort();

            // now find all files
            int bs = foldersList.Count / Math.Min(16, foldersList.Count);
            NBConsole.Log($"FileSizeCalclator.BatchAndProcessAllFiles top level folders: foldersList: {foldersList.Count}, bs: {bs}");

            Parallel.ForEach(Partitioner.Create(0, foldersList.Count, bs), range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    string[] fles = Directory.GetFiles(foldersList[i], "*", new EnumerationOptions() { IgnoreInaccessible = true, RecurseSubdirectories = true });
                    Parallel.ForEach(fles, x =>
                    {
                        try
                        {
                            FileInfo fi = new FileInfo(x);
                            FileHolder fh = new FileHolder() { Name = x, Size = fi.Length };
                            allfiles.Enqueue(fh);
                        } //try
                        catch (Exception ex)
                        {
                            Console.WriteLine($"{GetType().Name}.BatchAndProcessAllFiles Warning: {ex}");
                        }
                    });
                } //for
            }); //Parallel

        }

        /// <summary>
        /// Batch and split top level (3) folders and top level files.
        /// </summary>
        private void BatchAndSplitTopLevel(string folder, ConcurrentQueue<FileHolder> allfiles, ConcurrentQueue<string> folders)
        {
            // first 3 level folders.
            //folders.Enqueue(folder);
            Batch(folder, allfiles);

            string[] dirs = Directory.GetDirectories(folder, "*", new EnumerationOptions() { IgnoreInaccessible = true, RecurseSubdirectories = false });           // lvl1
            Parallel.ForEach(dirs, x =>
            {
                Batch(x, allfiles);
                //folders.Enqueue(x);
                string[] dirs2 = Directory.GetDirectories(x, "*", new EnumerationOptions() { IgnoreInaccessible = true, RecurseSubdirectories = false });           // lvl2
                Parallel.ForEach(dirs2, x2 =>
                {
                    Batch(x2, allfiles);
                    //folders.Enqueue(x2);
                    string[] dirs3 = Directory.GetDirectories(x2, "*", new EnumerationOptions() { IgnoreInaccessible = true, RecurseSubdirectories = false });      // lvl3
                    Parallel.ForEach(dirs3, x3 => folders.Enqueue(x3));
                });

            });

        }

        /// <summary>
        /// Batch top level files only.
        /// </summary>
        public void Batch(string folder, ConcurrentQueue<FileHolder> allfiles)
        {
            string[] fles = Directory.GetFiles(folder, "*", new EnumerationOptions() { IgnoreInaccessible = true, RecurseSubdirectories = false });
            Parallel.ForEach(fles, x =>
            {
                try
                {
                    FileInfo fi = new FileInfo(x);
                    FileHolder fh = new FileHolder() { Name = x, Size = fi.Length };
                    allfiles.Enqueue(fh);
                } //try
                catch (Exception ex)
                {
                    Console.WriteLine($"{GetType().Name}.Batch Warning: {ex}");
                }
            });
        }

        /// <summary>
        /// Format file size.
        /// </summary>
        public static string FormatFileSize(long fileSize) { return FormatFileSize((ulong)fileSize); }

        /// <summary>
        /// Format file size.
        /// </summary>
        public static string FormatFileSize(ulong fileSize)
        {

            double len = (double)fileSize;
            int order = 0;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order += 1;
                len /= 1024.0d;
            }

            return String.Format("{0:0.##} {1}", len, sizes[order]);
        }

    }
}
