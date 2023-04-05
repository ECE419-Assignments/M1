class How extends Thread {
    String wow = "wow";
    public void run(){
        
        while(true){
            System.out.println("RUN"
                + wow);
            try{
                Thread.sleep(5000);
            } catch (Exception ex){

            }
        }
    }

    public void help(){
        while(true){
            this.wow = "wowzers";
            System.out.println("HELP "
                + Thread.currentThread().getName());
            try{
                Thread.sleep(10000);
            } catch (Exception ex) {
            }
        }
    }

    public static void main(String[] args){
        How why = new How();
        why.start();
        try{
                Thread.sleep(10000);
            } catch (Exception ex) {
            }
        why.help();

    }
        
}

