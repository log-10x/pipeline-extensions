package com.log10x.ext.cloud.github;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.github.GHBranch;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHPullRequest;
import org.kohsuke.github.GHRef;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHTree;
import org.kohsuke.github.GHTreeBuilder;
import org.kohsuke.github.GitHub;
import org.kohsuke.github.GitHubBuilder;
import org.kohsuke.github.HttpException;

import com.log10x.api.util.MapperUtil;

public class GitHubOutputStream extends ByteArrayOutputStream {

	private static final byte[] LINE_BREAK_BYTES = System.lineSeparator().getBytes();
	
	private static final Logger logger = LogManager.getLogger(GitHubOutputStream.class);

	public static class GitHubOutputOptions {
				
		public final String githubOutputToken;
		
		public final String githubOutputRepo;
		public final String githubOutputBranch;
		public final String githubOutputPath;
		
		public final String githubOutputHeader;
		public final String githubOutputMessage;
		public final String githubOutputPullRequestBody;
		public final boolean githubOutputSquashMerge;

		public GitHubOutputOptions(
			String githubOutputToken,
			String githubOutputRepo,
			String githubOutputBranch,
			String githubOutputPath,
			String githubOutputHeader,
			String githubOutputMessage,
			String githubOutputPullRequestBody,
			boolean githubOutputSquashMerge) {
			
			this.githubOutputToken = githubOutputToken;
			this.githubOutputRepo = githubOutputRepo;
			this.githubOutputBranch = githubOutputBranch;
			this.githubOutputPath = githubOutputPath;
			this.githubOutputHeader = githubOutputHeader;
			this.githubOutputMessage = githubOutputMessage;
			this.githubOutputPullRequestBody = githubOutputPullRequestBody;
			this.githubOutputSquashMerge = githubOutputSquashMerge;
		}
		
		protected GitHubOutputOptions() {
			this(null, null, null, null, null, null, null, false);
		}
	}
	
	protected final GitHubOutputOptions options;
	
	protected final GHRepository repo;
		
	public GitHubOutputStream(Map<String, Object> args) throws IOException{
		
		this(MapperUtil.noFailUnknownJsonMapper.convertValue(args,
			GitHubOutputOptions.class));
	}
	
	public GitHubOutputStream(GitHubOutputOptions options) throws IOException {
		
		this.options = options;
					
		GitHub github = new GitHubBuilder().
			withOAuthToken(options.githubOutputToken).
			build();
		
		this.repo = github.getRepository(options.githubOutputRepo);
		
		if (options.githubOutputHeader != null) {	
			
			this.writeBytes(options.githubOutputHeader.getBytes());
			this.writeBytes(LINE_BREAK_BYTES);
		}
	}

	@Override
	public synchronized void flush() throws IOException {
		if (this.size() > 0) {
			this.writeBytes(LINE_BREAK_BYTES);
		}
		
		super.flush();
	}
	
	@Override
	public void close() throws IOException {
		
		String branchName = (options.githubOutputBranch != null && !options.githubOutputBranch.isBlank()) ?
			options.githubOutputBranch :
			repo.getDefaultBranch();
		
		String message = (options.githubOutputMessage != null && !options.githubOutputMessage.isBlank()) ?
				options.githubOutputMessage :
				"Updating " + options.githubOutputPath;
		
		try {
						
			String uuid = UUID.randomUUID().toString();
			
			String safeFileName = options.githubOutputPath.replace('.', '-').replace('/', '-').replace('\\', '-').replace(" ", "");
			String newBranchName = "update-" + safeFileName + "-" + uuid;

			GHBranch sourceBranch = repo.getBranch(branchName);

			GHRef newBranchRef = repo.createRef("refs/heads/" + newBranchName, sourceBranch.getSHA1());
			GHCommit newBranchCommit = repo.getCommit(newBranchRef.getObject().getSha());

			GHTree tree = newBranchCommit.getTree();
			GHTreeBuilder builder = repo.createTree().baseTree(tree.getSha());
					
			byte[] bytes = new byte[this.count];
			
			System.arraycopy(this.buf, 0, bytes, 0 ,bytes.length);

			builder.add(options.githubOutputPath, bytes, false);

			String treeSha = builder.create().getSha();

			String commitSha = repo.createCommit().
				message(message).
				tree(treeSha).
				parent(newBranchRef.getObject().getSha()).create().
				getSHA1();

			newBranchRef.updateTo(commitSha);

			GHPullRequest pr = repo.createPullRequest("Update " + 
				options.githubOutputPath, newBranchName, branchName,
				options.githubOutputPullRequestBody);

			try {
				
				GHPullRequest.MergeMethod method = (options.githubOutputSquashMerge ? GHPullRequest.MergeMethod.SQUASH : GHPullRequest.MergeMethod.MERGE);
				
				pr.merge("Update: " + options.githubOutputPath, null, method);
				
				if (!repo.isDeleteBranchOnMerge()) {
					newBranchRef.delete();
				}
				
				if (logger.isDebugEnabled()) { 
					
					logger.debug("Committed" +
						". file: " + options.githubOutputPath + 
						". branch: " + branchName + 
						". bytes: " + this.count
						);
				}
								
			} catch (HttpException e) {
				
				if (e.getResponseCode() == 405) {
					
					pr.close();
					newBranchRef.delete();
				}

				throw e;
			}
			
		} catch (Exception e) {
			
			throw new IllegalStateException("error comitting to GitHub" +
				". file: " + options.githubOutputPath + 
				". branch: " + branchName + 
				". count: "  + this.count , e
			);	
		}
	}
}
