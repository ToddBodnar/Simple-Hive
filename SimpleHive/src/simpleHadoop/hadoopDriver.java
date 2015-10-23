/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpleHadoop;

/**
 * A driver to run mrJobs.
 * @author toddbodnar
 */
public class hadoopDriver {
    private static long lastUpdate = -1;
    private static void log(int mapCt, int reduceCt, int totalMap, int totalReduce, boolean doingMap, boolean verbose)
    {
        if(System.currentTimeMillis() - lastUpdate < 500)
            return;
        if(!verbose)
            return;
        
        if(doingMap)
        {
            if((100*mapCt/totalMap)==(100*(mapCt-1)/totalMap))
                return;
        }
        else
        {
            if(totalReduce==0 || (100*reduceCt/totalReduce)==(100*(reduceCt-1)/totalReduce))
                return;
        }
        lastUpdate = System.currentTimeMillis();
        System.out.println(System.currentTimeMillis()+"\tMap "+(100*mapCt/totalMap)+"% \tReduce "+(100*reduceCt/totalReduce)+"%");
    }
    
    /**
     * Runs a job
     * @param theJob the mrJob to be run
     * @param verbose if true, output progress information
     */
    public static void run(mrJob theJob, boolean verbose)
    {
        context cont = new context();
        //if(verbose)
          //  System.out.println("Init "+theJob.getClass().toString());
        theJob.init(cont);
        int mapCt=0;
        int reduceCt=0;
        //if(verbose)
          //  System.out.println("Map "+theJob.getClass().toString());
        for(Object o:cont.toProcess)
        {
            theJob.map(o, cont);
            mapCt++;
            log(mapCt,reduceCt,cont.toProcess.size(),1,true,verbose);
        }
        //if(verbose)
          //  System.out.println("Reduce "+theJob.getClass().toString());
        for(Object o:cont.data.keySet())
        {
            theJob.reduce(o, cont.data.get(o));
            reduceCt++;
            log(mapCt,reduceCt,cont.toProcess.size(),cont.data.keySet().size(),false,verbose);
        }
        
        lastUpdate = -1;
        log(mapCt,reduceCt,cont.toProcess.size(),cont.data.keySet().size(),false,verbose);

    }
}
