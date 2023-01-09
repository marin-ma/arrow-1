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

#include <qatzip.h>

#define QZ_INIT_FAIL(rc) (QZ_OK != rc && QZ_DUPLICATE != rc)

#define QZ_SETUP_SESSION_FAIL(rc) \
  (QZ_PARAMS == rc || QZ_NOSW_NO_HW == rc || QZ_NOSW_LOW_MEM == rc)

class QatCodec : public Codec {
 protected:
  explicit QatCodec(QzPollingMode_T polling_mode) : polling_mode_(polling_mode) {}

  Result<int64_t> Decompress(int64_t input_len, const uint8_t* input,
                             int64_t output_buffer_len, uint8_t* output_buffer) override {
    uint32_t compressed_size = static_cast<uint32_t>(input_len);
    uint32_t uncompressed_size = static_cast<uint32_t>(output_buffer_len);
    int ret = qzDecompress(&qzSession, input, &compressed_size, output_buffer,
                           &uncompressed_size);
    if (ret == QZ_OK) {
      return static_cast<int64_t>(uncompressed_size);
    } else if (ret == QZ_PARAMS) {
      return Status::IOError("QAT decompression failure: params is invalid");
    } else if (ret == QZ_FAIL) {
      return Status::IOError("QAT decompression failure: Function did not succeed");
    } else {
      return Status::IOError("QAT decompression failure with error:", ret);
    }
  }

  int64_t MaxCompressedLen(int64_t input_len,
                           const uint8_t* ARROW_ARG_UNUSED(input)) override {
    DCHECK_GE(input_len, 0);
    return qzMaxCompressedLength(static_cast<size_t>(input_len), &qzSession);
  }

  Result<int64_t> Compress(int64_t input_len, const uint8_t* input,
                           int64_t output_buffer_len, uint8_t* output_buffer) override {
    uint32_t uncompressed_size = static_cast<uint32_t>(input_len);
    uint32_t compressed_size = static_cast<uint32_t>(output_buffer_len);
    int ret = qzCompress(&qzSession, input, &uncompressed_size, output_buffer,
                         &compressed_size, 1);
    if (ret == QZ_OK) {
      return static_cast<int64_t>(compressed_size);
    } else if (ret == QZ_PARAMS) {
      return Status::IOError("QAT compression failure: params is invalid");
    } else if (ret == QZ_FAIL) {
      return Status::IOError("QAT compression failure: function did not succeed");
    } else {
      return Status::IOError("QAT compression failure with error:", ret);
    }
  }

  Result<std::shared_ptr<Compressor>> MakeCompressor() override {
    return Status::NotImplemented("Streaming compression unsupported with QAT");
  }

  Result<std::shared_ptr<Decompressor>> MakeDecompressor() override {
    return Status::NotImplemented("Streaming decompression unsupported with QAT");
  }

  QzSession_T qzSession = {0};
  QzPollingMode_T polling_mode_;
};
