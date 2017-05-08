require 'azure'

class ::File
  def each_chunk(chunk_size=2**20) # 1mb chunks
    yield read(chunk_size) until eof?
  end
end

module CarrierWave
  module Storage
    class Azure < Abstract
      def store!(file)
        azure_file = CarrierWave::Storage::Azure::File.new(uploader, connection, uploader.store_path)
        azure_file.store!(file)
        azure_file
      end

      def retrieve!(identifer)
        CarrierWave::Storage::Azure::File.new(uploader, connection, uploader.store_path(identifer))
      end

      def connection
        @connection ||= begin
          %i(storage_account_name storage_access_key storage_blob_host).each do |key|
            ::Azure.config.send("#{key}=", uploader.send("azure_#{key}"))
          end
          ::Azure::Blob::BlobService.new
        end
      end

      class File
        attr_reader :path, :block_list

        def initialize(uploader, connection, path)
          @uploader = uploader
          @connection = connection
          @path = path
          @counter = 1
          @block_list = []
        end

        def store!(file)
          file.to_file.each_chunk do |chunk|
            Rails.logger.info { "CHUNK #{@counter}" }
            block_id = @counter.to_s.rjust(5, '0')
            block_list << [block_id, :uncommitted]

            options = {
              content_md5: Base64.strict_encode64(Digest::MD5.digest(chunk)),
              timeout:     300 # 5 minute timeout
            }

            @connection.create_blob_block @uploader.azure_container, @path, block_id, chunk, options
            @counter += 1
          end

          @connection.commit_blob_blocks(@uploader.azure_container, @path, block_list, { blob_content_type: file.content_type })
          true
        end

        def url(options = {})
          path = ::File.join @uploader.azure_container, @path
          if @uploader.asset_host
            "#{@uploader.asset_host}/#{path}"
          else
            @connection.generate_uri(path).to_s
          end
        end

        def read
          content
        end

        def content_type
          @content_type = blob.properties[:content_type] if @content_type.nil? && !blob.nil?
          @content_type
        end

        def content_type=(new_content_type)
          @content_type = new_content_type
        end

        def exitst?
          blob.nil?
        end

        def size
          blob.properties[:content_length] unless blob.nil?
        end

        def filename
          URI.decode(url).gsub(/.*\/(.*?$)/, '\1')
        end

        def extension
          @path.split('.').last
        end

        def delete
          begin
            @connection.delete_blob @uploader.azure_container, @path
            true
          rescue ::Azure::Core::Http::HTTPError
            false
          end
        end

        private

        def blob
          load_content if @blob.nil?
          @blob
        end

        def content
          load_content if @content.nil?
          @content
        end

        def load_content
          @blob, @content = begin
            @connection.get_blob @uploader.azure_container, @path
          rescue ::Azure::Core::Http::HTTPError
          end
        end
      end
    end
  end
end
