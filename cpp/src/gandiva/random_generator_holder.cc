// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "gandiva/random_generator_holder.h"
#include "gandiva/node.h"
#include "gandiva/projector.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "gandiva/tree_expr_builder.h"
//#include "gandiva/tests/test_util.h"
#include "arrow/type_traits.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_base.h"
#include <type_traits>


namespace gandiva {
Status RandomGeneratorHolder::Make(const FunctionNode& node,
                                   std::shared_ptr<RandomGeneratorHolder>* holder) {
  ARROW_RETURN_IF(node.children().size() > 2,
                  Status::Invalid("'random' function requires at most two parameters"));

  if (node.children().size() == 0) {
    *holder = std::shared_ptr<RandomGeneratorHolder>(new RandomGeneratorHolder());
    return Status::OK();
  }

  auto literal = dynamic_cast<LiteralNode*>(node.children().at(0).get());
  //ARROW_RETURN_IF(literal == nullptr,
  //                Status::Invalid("'random' function requires a literal as parameter"));
  int64_t seed;
  if (literal != nullptr) {
    auto literal_type = literal->return_type()->id();
    ARROW_RETURN_IF(
      literal_type != arrow::Type::INT32 && literal_type != arrow::Type::INT64,
      Status::Invalid("'random' function requires an int32/int64 literal as parameter"));
    if (literal_type == arrow::Type::INT32) {
      seed = literal->is_null() ? 0 : arrow::util::get<int32_t>(literal->holder());
    } else {
      seed = literal->is_null() ? 0 : arrow::util::get<int64_t>(literal->holder());
    }
  } else {
    auto first_children_node_ptr = node.children().at(0);
    //auto schema = arrow::schema({});
    auto f0 = arrow::field("f0", arrow::float64());
    auto schema = arrow::schema({f0});
    std::shared_ptr<Projector> projector;
    // Not actually used.
    auto res = field("res", arrow::int32());
    auto expr = TreeExprBuilder::MakeExpression(first_children_node_ptr, res);
    auto builder = ConfigurationBuilder();
    auto config = builder.DefaultConfiguration();
    auto status = Projector::Make(schema, {expr}, config, &projector);
    arrow::ArrayVector outputs;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    
    //arrow::ArrayVector inputs;
    //auto in_batch = arrow::RecordBatch::Make(schema, 0, inputs);
    
    std::vector<int> input0 = {16, 10, -14, 8};
    std::vector<bool> validity = {true, true, true, true};
    std::shared_ptr<arrow::Array> array0;
    //arrow::ArrayFromVector<arrow::DoubleType, double>(validity, values, &array0);
    //auto array0 = MakeArrowArray<arrow::DoubleType, double>(input0, validity);
    
    auto type = arrow::TypeTraits<arrow::Int32Type>::type_singleton();
    std::unique_ptr<arrow::ArrayBuilder> builder_ptr;
    MakeBuilder(pool, type, &builder_ptr);
    auto& arrow_array_builder = dynamic_cast<typename arrow::TypeTraits<arrow::Int32Type>::BuilderType&>(*builder_ptr);
    for (size_t i = 0; i < input0.size(); ++i) {
      arrow_array_builder.Append(input0[i]);
    }
    arrow_array_builder.Finish(&array0);   
 
    auto in_batch = arrow::RecordBatch::Make(schema, 4, {array0});
   
    //arrow::RecordBatch in_batch;
    projector->Evaluate(*in_batch, pool, &outputs);
    auto result_arr = std::dynamic_pointer_cast<arrow::Int32Array>(outputs.at(0));
    //seed = dynamic_cast<int32_t>(result_arr->Value(0));
    seed = result_arr->Value(0);
  }
  // The offset is a partition ID in spark SQL. It is used to achieve genuine random distribution globally.
  int32_t offset = 0;
  if (node.children().size() > 1) {
    auto offset_node = dynamic_cast<LiteralNode*>(node.children().at(1).get());
    offset = offset_node->is_null() ? 0 : arrow::util::get<int32_t>(offset_node->holder());
  }
  *holder = std::shared_ptr<RandomGeneratorHolder>(new RandomGeneratorHolder(seed + offset));
  return Status::OK();
}
}  // namespace gandiva
