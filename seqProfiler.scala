/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
import com.twitter.scalding._

/**
This code was based on the scalding tutorials included in the repo on github:  Scalding tutorial part 4.

  Run:
    scripts/scald.rb \
      --local seqProfiler.scala \
      --input data/hello.txt \
      --output data/output.txt

  Check the output:
    cat data/output.txt

**/

class seqProfiler(args : Args) extends Job(args) {

  TextLine(args("input")) 
 // Tsv(args("input"), ('header, 'sequence))
    .read
    .flatMap('line -> 'word){ line : String => line.sliding(2).toList}

    /**
    To count the words, first we need to group by word.
    groupBy takes any number of fields as the group key. In this
    case we just want 'word.

    groupBy also takes an anonymous function, to which it will pass a
    com.twitter.scalding.GroupBuilder.

    Each method call to GroupBuilder will specify an aggregation we want to
    perform on the group. In general, the resulting data stream will have all
    of the group fields (with one entry for each set of unique values), plus
    one new field for each aggregation.

    In this case, the only aggregation we care about is size: how many values are
    in the group.
    **/

    .groupBy('word){group => group.size}

    /**
    No project is needed here because the groupBy has eliminated everything but 'word
    and the size field.
    **/

    .write(Tsv(args("output")))
}
