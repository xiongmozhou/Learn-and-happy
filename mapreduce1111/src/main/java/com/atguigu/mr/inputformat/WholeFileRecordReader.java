package com.atguigu.mr.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {


    private boolean isRead = false;

    //Key,Value提升为Fields
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    //流必须为成员变量
    private FileSystem fileSystem;
    private FSDataInputStream fis;
    private FileSplit fs;


    /**
     * 初始化方法，框架会自动调用一次
     *
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //开流
        fileSystem = FileSystem.get(context.getConfiguration());
        fs = (FileSplit) split;
        Path path = fs.getPath();
        fis = fileSystem.open(path);

    }

    /**
     * 读取下一组KV值
     * @return true 表示读到了，false表示文件读完了
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //判断文件是否被读取过
        if (!isRead) {
            //读文件，读到KV值里
            //key=????, value=?????

            //获取全路径，并封装进key
            String path = fs.getPath().toString();
            key.set(path);

            //获取文件长度，以确定缓冲区的大小
            long length = fs.getLength();
            byte[] b = new byte[(int) length];

            //获取文件全部内容，并封装进value
            fis.read(b);
            value.set(b, 0, b.length);


            //标记我们的文件读过了
            isRead = true;
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取读到的Key
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取读到的Value
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 返回数据读取进度
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public float getProgress() throws IOException, InterruptedException {
        return isRead ? 1 : 0;
    }

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    public void close() throws IOException {
        fileSystem.close();
        IOUtils.closeStream(fis);
    }
}
