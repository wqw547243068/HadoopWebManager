# HadoopWebManager
A web ui to manage hdfs and simple mapreduce job
## markdown画图指南
参考[github上的指南](http://knsv.github.io/mermaid/#flowcharts-basic-syntax)
- 1 从做到右画
```
graph LR
A[左边]-->B(右边)
```
- 从上到下
```
graph TB
A((上边))-->|标注|B(下边)
```
- 子图

```
 %% Subgraph example
 graph TB
         subgraph one
         a1-->a2
         end
         subgraph two
         b1-->b2
         end
         subgraph three
         c1-->c2
         end
         c1-->a2
```
- fu
```
    A[Hard edge] -->|Link text| B(Round edge)
    B --> C{Decision}
    C -->|One| D[Result one]
    C -->|Two| E[Result two]
```
- 序列图
```
%% Example of sequence diagram
sequenceDiagram
    Alice->>John: Hello John, how are you?
    John-->>Alice: Great!
```
- 甘特图
```
%% Example of sequence diagram
gantt
    title A Gantt Diagram

    section Section
    A task           :a1, 2014-01-01, 30d
    Another task     :after a1  , 20d
    section Another
    Task in sec      :2014-01-12  , 12d
    anther task      : 24d
```
