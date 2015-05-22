// This file was automatically generated with the by org.lemurproject.galago.tupleflow.typebuilder.TypeBuilderMojo ...
package org.lemurproject.galago.core.types;

import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.protocol.*;
import org.lemurproject.galago.tupleflow.error.*;
import org.lemurproject.galago.utility.*;
import java.io.*;
import java.util.*;
import gnu.trove.list.array.*;

/**
 * Tupleflow-Typebuilder automatically-generated class: WordCount.
 */
@SuppressWarnings("unused")
public final class WordCount implements Type<WordCount> {
    public byte[] word;
    public long collectionFrequency;
    public long documentCount;
    public long maxDocumentFrequency; 
    
    /** default constructor makes most fields null */
    public WordCount() {}
    /** additional constructor takes all fields explicitly */
    public WordCount(byte[] word, long collectionFrequency, long documentCount, long maxDocumentFrequency) {
        this.word = word;
        this.collectionFrequency = collectionFrequency;
        this.documentCount = documentCount;
        this.maxDocumentFrequency = maxDocumentFrequency;
    }  
    
    public String toString() {
        try {
            return String.format("%s,%d,%d,%d",
                                   new String(word, "UTF-8"), collectionFrequency, documentCount, maxDocumentFrequency);
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException("Couldn't convert string to UTF-8.");
        }
    } 

    public Order<WordCount> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+word" })) {
            return new WordOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<WordCount> {
        public void process(WordCount object) throws IOException;
    } 
    public interface Source extends Step {
    }
    public static final class WordOrder implements Order<WordCount> {
        public int hash(WordCount object) {
            int h = 0;
            h += CmpUtil.hash(object.word);
            return h;
        } 
        public Comparator<WordCount> greaterThan() {
            return new Comparator<WordCount>() {
                public int compare(WordCount one, WordCount two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.word, two.word);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<WordCount> lessThan() {
            return new Comparator<WordCount>() {
                public int compare(WordCount one, WordCount two) {
                    int result = 0;
                    do {
                        result = + CmpUtil.compare(one.word, two.word);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<WordCount> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<WordCount> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<WordCount> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static final class OrderedWriterClass extends OrderedWriter< WordCount > {
            WordCount last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(WordCount object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != CmpUtil.compare(object.word, last.word)) { processAll = true; shreddedWriter.processWord(object.word); }
               shreddedWriter.processTuple(object.collectionFrequency, object.documentCount, object.maxDocumentFrequency);
               last = object;
            }           

            @Override
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<WordCount> getInputClass() {
                return WordCount.class;
            }
        } 
        public ReaderSource<WordCount> orderedCombiner(Collection<TypeReader<WordCount>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList<ShreddedReader>();
            
            for (TypeReader<WordCount> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public WordCount clone(WordCount object) {
            WordCount result = new WordCount();
            if (object == null) return result;
            result.word = object.word; 
            result.collectionFrequency = object.collectionFrequency; 
            result.documentCount = object.documentCount; 
            result.maxDocumentFrequency = object.maxDocumentFrequency; 
            return result;
        }                 
        public Class<WordCount> getOrderedClass() {
            return WordCount.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+word"};
        }

        public static String[] getSpec() {
            return new String[] {"+word"};
        }
        public static String getSpecString() {
            return "+word";
        }
                           
        public interface ShreddedProcessor extends Step, Closeable {
            public void processWord(byte[] word) throws IOException;
            public void processTuple(long collectionFrequency, long documentCount, long maxDocumentFrequency) throws IOException;
        } 

        public static final class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            byte[] lastWord;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        

            @Override
            public void close() throws IOException {
                flush();
            }
            
            public void processWord(byte[] word) {
                lastWord = word;
                buffer.processWord(word);
            }
            public final void processTuple(long collectionFrequency, long documentCount, long maxDocumentFrequency) throws IOException {
                if (lastFlush) {
                    if(buffer.words.size() == 0) buffer.processWord(lastWord);
                    lastFlush = false;
                }
                buffer.processTuple(collectionFrequency, documentCount, maxDocumentFrequency);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeLong(buffer.getCollectionFrequency());
                    output.writeLong(buffer.getDocumentCount());
                    output.writeLong(buffer.getMaxDocumentFrequency());
                    buffer.incrementTuple();
                }
            }  
            public final void flushWord(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getWordEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeBytes(buffer.getWord());
                    output.writeInt(count);
                    buffer.incrementWord();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushWord(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static final class ShreddedBuffer {
            ArrayList<byte[]> words = new ArrayList<byte[]>();
            TIntArrayList wordTupleIdx = new TIntArrayList();
            int wordReadIdx = 0;
                            
            long[] collectionFrequencys;
            long[] documentCounts;
            long[] maxDocumentFrequencys;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                collectionFrequencys = new long[batchSize];
                documentCounts = new long[batchSize];
                maxDocumentFrequencys = new long[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processWord(byte[] word) {
                words.add(word);
                wordTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(long collectionFrequency, long documentCount, long maxDocumentFrequency) {
                assert words.size() > 0;
                collectionFrequencys[writeTupleIndex] = collectionFrequency;
                documentCounts[writeTupleIndex] = documentCount;
                maxDocumentFrequencys[writeTupleIndex] = maxDocumentFrequency;
                writeTupleIndex++;
            }
            public void resetData() {
                words.clear();
                wordTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                wordReadIdx = 0;
            } 

            public void reset() {
                resetData();
                resetRead();
            } 
            public boolean isFull() {
                return writeTupleIndex >= batchSize;
            }

            public boolean isEmpty() {
                return writeTupleIndex == 0;
            }                          

            public boolean isAtEnd() {
                return readTupleIndex >= writeTupleIndex;
            }           
            public void incrementWord() {
                wordReadIdx++;  
            }                                                                                              

            public void autoIncrementWord() {
                while (readTupleIndex >= getWordEndIndex() && readTupleIndex < writeTupleIndex)
                    wordReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getWordEndIndex() {
                if ((wordReadIdx+1) >= wordTupleIdx.size())
                    return writeTupleIndex;
                return wordTupleIdx.get(wordReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public byte[] getWord() {
                assert readTupleIndex < writeTupleIndex;
                assert wordReadIdx < words.size();
                
                return words.get(wordReadIdx);
            }
            public long getCollectionFrequency() {
                assert readTupleIndex < writeTupleIndex;
                return collectionFrequencys[readTupleIndex];
            }                                         
            public long getDocumentCount() {
                assert readTupleIndex < writeTupleIndex;
                return documentCounts[readTupleIndex];
            }                                         
            public long getMaxDocumentFrequency() {
                assert readTupleIndex < writeTupleIndex;
                return maxDocumentFrequencys[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getCollectionFrequency(), getDocumentCount(), getMaxDocumentFrequency());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexWord(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processWord(getWord());
                    assert getWordEndIndex() <= endIndex;
                    copyTuples(getWordEndIndex(), output);
                    incrementWord();
                }
            }  
            public void copyUntilWord(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + CmpUtil.compare(getWord(), other.getWord());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processWord(getWord());
                                      
                        copyTuples(getWordEndIndex(), output);
                    } else {
                        output.processWord(getWord());
                        copyTuples(getWordEndIndex(), output);
                    }
                    incrementWord();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilWord(other, output);
            }
            
        }                         
        public static final class ShreddedCombiner implements ReaderSource<WordCount>, ShreddedSource {
            public ShreddedProcessor processor;
            Collection<ShreddedReader> readers;       
            boolean closeOnExit = false;
            boolean uninitialized = true;
            PriorityQueue<ShreddedReader> queue = new PriorityQueue<ShreddedReader>();
            
            public ShreddedCombiner(Collection<ShreddedReader> readers, boolean closeOnExit) {
                this.readers = readers;                                                       
                this.closeOnExit = closeOnExit;
            }
                                  
            public void setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof WordCount.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((WordCount.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<WordCount>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<WordCount> getOutputClass() {
                return WordCount.class;
            }
            
            public void initialize() throws IOException {
                for (ShreddedReader reader : readers) {
                    reader.fill();                                        
                    
                    if (!reader.getBuffer().isAtEnd())
                        queue.add(reader);
                }   

                uninitialized = false;
            }

            public void run() throws IOException {
                initialize();
               
                while (queue.size() > 0) {
                    ShreddedReader top = queue.poll();
                    ShreddedReader next = null;
                    ShreddedBuffer nextBuffer = null; 
                    
                    assert !top.getBuffer().isAtEnd();
                                                  
                    if (queue.size() > 0) {
                        next = queue.peek();
                        nextBuffer = next.getBuffer();
                        assert !nextBuffer.isAtEnd();
                    }
                    
                    top.getBuffer().copyUntil(nextBuffer, processor);
                    if (top.getBuffer().isAtEnd())
                        top.fill();                 
                        
                    if (!top.getBuffer().isAtEnd())
                        queue.add(top);
                }              
                
                if (closeOnExit)
                    processor.close();
            }

            public WordCount read() throws IOException {
                if (uninitialized)
                    initialize();

                WordCount result = null;

                while (queue.size() > 0) {
                    ShreddedReader top = queue.poll();
                    result = top.read();

                    if (result != null) {
                        if (top.getBuffer().isAtEnd())
                            top.fill();

                        queue.offer(top);
                        break;
                    } 
                }

                return result;
            }
        } 
        public static final class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<WordCount>, ShreddedSource {
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            WordCount last = new WordCount();         
            long updateWordCount = -1;
            long tupleCount = 0;
            long bufferStartCount = 0;  
            ArrayInput input;
            
            public ShreddedReader(ArrayInput input) {
                this.input = input; 
                this.buffer = new ShreddedBuffer();
            }                               
            
            public ShreddedReader(ArrayInput input, int bufferSize) { 
                this.input = input;
                this.buffer = new ShreddedBuffer(bufferSize);
            }
                 
            public final int compareTo(ShreddedReader other) {
                ShreddedBuffer otherBuffer = other.getBuffer();
                
                if (buffer.isAtEnd() && otherBuffer.isAtEnd()) {
                    return 0;                 
                } else if (buffer.isAtEnd()) {
                    return -1;
                } else if (otherBuffer.isAtEnd()) {
                    return 1;
                }
                                   
                int result = 0;
                do {
                    result = + CmpUtil.compare(buffer.getWord(), otherBuffer.getWord());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final WordCount read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                WordCount result = new WordCount();
                
                result.word = buffer.getWord();
                result.collectionFrequency = buffer.getCollectionFrequency();
                result.documentCount = buffer.getDocumentCount();
                result.maxDocumentFrequency = buffer.getMaxDocumentFrequency();
                
                buffer.incrementTuple();
                buffer.autoIncrementWord();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateWordCount - tupleCount > 0) {
                            buffer.words.add(last.word);
                            buffer.wordTupleIdx.add((int) (updateWordCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateWord();
                        buffer.processTuple(input.readLong(), input.readLong(), input.readLong());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateWord() throws IOException {
                if (updateWordCount > tupleCount)
                    return;
                     
                last.word = input.readBytes();
                updateWordCount = tupleCount + input.readInt();
                                      
                buffer.processWord(last.word);
            }

            public void run() throws IOException {
                while (true) {
                    fill();
                    
                    if (buffer.isAtEnd())
                        break;
                    
                    buffer.copyUntil(null, processor);
                }      
                processor.close();
            }
            
            public void setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof WordCount.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((WordCount.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<WordCount>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<WordCount> getOutputClass() {
                return WordCount.class;
            }                
        }
        
        public static final class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            WordCount last = new WordCount();
            boolean wordProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processWord(byte[] word) throws IOException {  
                if (wordProcess || CmpUtil.compare(word, last.word) != 0) {
                    last.word = word;
                    processor.processWord(word);
                    wordProcess = false;
                }
            }  
            
            public void resetWord() {
                 wordProcess = true;
            }                                                
                               
            public void processTuple(long collectionFrequency, long documentCount, long maxDocumentFrequency) throws IOException {
                processor.processTuple(collectionFrequency, documentCount, maxDocumentFrequency);
            } 

            @Override
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static final class TupleUnshredder implements ShreddedProcessor {
            WordCount last = new WordCount();
            public org.lemurproject.galago.tupleflow.Processor<WordCount> processor;                               
            
            public TupleUnshredder(WordCount.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<WordCount> processor) {
                this.processor = processor;
            }
            
            public WordCount clone(WordCount object) {
                WordCount result = new WordCount();
                if (object == null) return result;
                result.word = object.word; 
                result.collectionFrequency = object.collectionFrequency; 
                result.documentCount = object.documentCount; 
                result.maxDocumentFrequency = object.maxDocumentFrequency; 
                return result;
            }                 
            
            public void processWord(byte[] word) throws IOException {
                last.word = word;
            }   
                
            
            public void processTuple(long collectionFrequency, long documentCount, long maxDocumentFrequency) throws IOException {
                last.collectionFrequency = collectionFrequency;
                last.documentCount = documentCount;
                last.maxDocumentFrequency = maxDocumentFrequency;
                processor.process(clone(last));
            }               

            @Override
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static final class TupleShredder implements Processor {
            WordCount last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public WordCount clone(WordCount object) {
                WordCount result = new WordCount();
                if (object == null) return result;
                result.word = object.word; 
                result.collectionFrequency = object.collectionFrequency; 
                result.documentCount = object.documentCount; 
                result.maxDocumentFrequency = object.maxDocumentFrequency; 
                return result;
            }                 
            
            public void process(WordCount object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || CmpUtil.compare(last.word, object.word) != 0 || processAll) { processor.processWord(object.word); processAll = true; }
                processor.processTuple(object.collectionFrequency, object.documentCount, object.maxDocumentFrequency);                                         
                last = object;
            }
                          
            public Class<WordCount> getInputClass() {
                return WordCount.class;
            }

            @Override
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    