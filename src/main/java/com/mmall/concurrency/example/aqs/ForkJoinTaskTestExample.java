package com.mmall.concurrency.example.aqs;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;

@Slf4j
public class ForkJoinTaskTestExample {

    public static final int threshold = 2;
    private int start;
    private int end;

    public ForkJoinTaskTestExample(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public void  test() {
        for (int i = start+1; i <=end ; i++) {
            start+=i;
        }
        System.out.println(start);
    }




    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        ForkJoinPool forkjoinPool = new ForkJoinPool();

        //生成一个计算任务，计算1+2+3+4
        ForkJoinTaskTestExample task = new ForkJoinTaskTestExample(1, 10000);
        task.test();
        long end = System.currentTimeMillis();
        long time = (end -start);

        try {
            log.info("time:{}", time);
        } catch (Exception e) {
            log.error("exception", e);
        }
    }

}
