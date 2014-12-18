var cluster         = require( 'cluster' );
var cCPUs           = require('os').cpus().length;

var mongojs 		= require('mongojs');
var db	            = mongojs('rssque:asta1404rssque@mongify.rssque.com:3932/rssque',['feeds','users', 'items']);

var http 			= require('http');
var https 			= require('https');
var request			= require('request');
var cheerio 		= require('cheerio');
var feedParser 		= require('feedparser');
var feedRequest		= require('request');
var Iconv 			= require('iconv').Iconv;

var read			= require('read-art');

var crypto 			= require('crypto');

var Q				= require('q');

var validator 		= require('validator');




if( cluster.isMaster ) {
	//cCPUs = 1;
    for( var i = 0; i < cCPUs; i++ ) {
        cluster.fork();
    }

    cluster.on( 'online', function( worker ) {
        console.log( 'Worker ' + worker.process.pid + ' is online.' );
    });
    cluster.on( 'exit', function( worker, code, signal ) {
        console.log( 'worker ' + worker.process.pid + ' died.' );
        //olen her birinin yerine yenisi gelebilir belki boyle
        cluster.fork();
    });
} else {
	setInterval(processFeed, 1000);
}

function processFeed(){
	var curDate = new Date();
	db.feeds.findAndModify(
		{
			query: { $or:
					    [
					        { ischecking: 0, nextrun: {$lt: new Date()}},
					        { nextrun: {$exists: false}}
					    ]
					},
			//query: {_id: mongojs.ObjectId("5483f8a2d3479736550005e7")},
			sort: { nextrun: 1 },
			update: { $set: { ischecking: 1 } },
			new: true
		},
		function(err, curFeed, lastErrorObject){
			if(err){
				console.log("We couldn't get the feed from mongo");
			} else {
				if(curFeed){
					if(validator.isURL(curFeed.url)){
						console.log("We have the feed:" + curFeed._id);
						var cururl = curFeed.url;
						if(cururl.substring(0,7) != 'http://'){
							if(cururl.substring(0,6) != 'ftp://'){
								if(cururl.substring(0,8) != 'https://'){
									cururl = 'http://' + cururl;
									db.feeds.update(
										{_id: curFeed._id}, 
										{$set: {url: cururl}},
										function(err, updated) {
											if( err || !updated ){
												console.log("We couldn't update the feed url, will try next time and let it go here:" + curFeed._id);
											} else {
												console.log("Feed url is now updated and next time we will not fall in here:" + curFeed._id);
											}
										}
									);
								}
							}
						}
						
						var frequest = feedRequest(cururl, {timeout: 10000, pool: false});
						frequest.setMaxListeners(50);
						frequest.setHeader('user-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36');
						frequest.setHeader('accept', 'text/html,application/xhtml+xml');
						//console.log(frequest.timeout);
						//frequest.timeout = 120000;
						//console.log(frequest.timeout);
						
						var feedParserOptions = [];
						feedParserOptions.addmeta = false;
						var fparser = new feedParser(feedParserOptions);
						
						frequest.on('error', function(error){
							console.log("There is an error with fetching the body:" + curFeed._id);
							console.log(error);
							console.log("We will now release the feed to be processed next time:" + curFeed._id);
							curDate.setHours(curDate.getHours()+24);
							db.feeds.update(
								{ _id: curFeed._id }, 
								{ $set: { ischecking: 0, nextrun: curDate, faultylink:1 } },
								function(err, updated) {
									if( err || !updated ){
										console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
										//processFeed();
									} else {
										console.log("Feed is now released:" + curFeed._id);
										//processFeed();
									}
								}
							);
						});
						frequest.on('response', function(res) {
							if (res.statusCode != 200){
								console.log("There is an error with response:" + curFeed._id);
								console.log("We will now release the feed to be processed next time:" + curFeed._id);
								curDate.setHours(curDate.getHours()+1);
								db.feeds.update(
									{ _id: curFeed._id }, 
									{ $set: { ischecking: 0, nextrun: curDate } },
									function(err, updated) {
										if( err || !updated ){
											console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
											//processFeed();
										} else {
											console.log("Feed is now released:" + curFeed._id);
											//processFeed();
										}
									}
								);
							}else{
								var charset = getParams(res.headers['content-type'] || '').charset;
								res = maybeTranslate(res, charset);
								// And boom goes the dynamite
								res.pipe(fparser);
							}
							
						});
						
						fparser.on('error', function(err){
							if(err == 'Error: Not a feed'){
								console.log('URL is not resolving to a feed:' + curFeed._id);
								console.log('We will now move this link to oldurl element and empty the url.');
								console.log('On the next run system will find try to find the correct url.');
								console.log('We will also be logging the issue to the feed document');
								
								curDate.setHours(curDate.getHours()-1);
								db.feeds.update(
									{ _id: curFeed._id }, 
									{ $set: { ischecking: 0, nextrun: curDate, oldurl: curFeed.url, url:'', thereisanissue:1 } },
									function(err, updated) {
										if( err || !updated ){
											console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
											//processFeed();
										} else {
											console.log("Feed is now released:" + curFeed._id);
											//processFeed();
										}
									}
								);
								
							} else {
								curDate.setHours(curDate.getHours()+24);
								db.feeds.update(
									{ _id: curFeed._id }, 
									{ $set: { ischecking: 0, nextrun: curDate } },
									function(err, updated) {
										if( err || !updated ){
											console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
											//processFeed();
										} else {
											console.log("Feed is now released:" + curFeed._id);
											//processFeed();
										}
									}
								);
							}
						});
						
						fparser.on('meta', function(meta) {
							
							var curFeedLink = meta.xmlurl;
							var curSiteLink = curFeed.site;
							
							if(!validator.isURL(curFeedLink) && curSiteLink){
								if( curSiteLink.slice(-1) == '/'){
									curSiteLink = curSiteLink.substring(0,curSiteLink.length-1);
								}
								curFeedLink = curSiteLink + curFeedLink;
							}
							
							if( (meta.title != curFeed.title || JSON.stringify(meta.categories) != JSON.stringify(curFeed.categories) || meta.favicon != curFeed.favicon || meta.link != curFeed.site)){
								db.feeds.update(
									{ _id: curFeed._id }, 
									{ 
										$set: { 
											title: 			meta.title,
											//url: 			curFeedLink,
											categories: 	meta.categories,
											favicon: 		meta.favicon,
											site: 			meta.link
										} 
									},
									function(err, updated) {
										if( err || !updated ){
											console.log("We tried to update the feed details, but failed:" + curFeed._id);
										} else {
											console.log("Feed details were not matching, so we updated:" + curFeed._id);
										}
									}
								);
							}
							
						});
						
						fparser.on('readable', function() {
							var post;
							
							while (post = this.read()) {
								
								//console.log(post.pubdate);
								var linkhash = crypto.createHash('sha1').update(post.link).digest('hex');
								
								//console.log(linkhash);
								//console.log(post.title);
								
								db.items.update(
									{ feed: curFeed._id, linkhash: linkhash }, 
									{$set: 
										{
											feed: curFeed._id,
											title: post.title,
											link: post.link,
											linkhash: linkhash,
											date: (post.pubdate || new Date()),
											content: post.description
										}
									}, 
									{upsert: true}, 
									function(err) {
										if(err){
											console.log("We couldn't insert the item");
										}
									}
								);
								
								
							}
						});
						
						fparser.on('end', function(){
							console.log("Feed parsing is now complete");
							db.items.find({feed:curFeed._id}, {date:1}).sort({date:1}).limit(1, function(fderror, firstdoc){
								if(fderror || !firstdoc){
									console.log("There is an issue on finding the first doc. We will set it to 24 hours later:" + curFeed._id);
									curDate.setHours(curDate.getHours()+24);
									db.feeds.update(
										{ _id: curFeed._id }, 
										{ $set: { ischecking: 0, nextrun: curDate } },
										function(err, updated) {
											if( err || !updated ){
												console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
												//processFeed();
											} else {
												console.log("Feed is now released:" + curFeed._id);
												//processFeed();
											}
										}
									);
								} else {
									db.items.find({feed:curFeed._id}, {date:1}).sort({date:-1}).limit(1, function(lderror, lastdoc){
										if(lderror || !lastdoc){
											console.log("There is an issue on finding the last doc. We will set it to 24 hours later:" + curFeed._id);
											curDate.setHours(curDate.getHours()+24);
											db.feeds.update(
												{ _id: curFeed._id }, 
												{ $set: { ischecking: 0, nextrun: curDate } },
												function(err, updated) {
													if( err || !updated ){
														console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
														//processFeed();
													} else {
														console.log("Feed is now released:" + curFeed._id);
														//processFeed();
													}
												}
											);
										} else {
											db.items.find({feed:curFeed._id}).count( function(tcerror, theCount){
												if(tcerror || !theCount){
													console.log("There is an issue on finding the count. We will set it to 24 hours later:" + curFeed._id);
													curDate.setHours(curDate.getHours()+24);
													db.feeds.update(
														{ _id: curFeed._id }, 
														{ $set: { ischecking: 0, nextrun: curDate } },
														function(err, updated) {
															if( err || !updated ){
																console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
																//processFeed();
															} else {
																console.log("Feed is now released:" + curFeed._id);
																//processFeed();
															}
														}
													);
												} else {
													var startDate = firstdoc[0].date;
													var closeDate = lastdoc[0].date;
													var minsDifference = (closeDate - startDate) / 1000 / 60;
													minsDifference = minsDifference / theCount;
													if(minsDifference < 60){
														minsDifference = 60;
													}
													if(minsDifference > 1440){
														minsDifference = 1440;
													}
													curDate.setMinutes(curDate.getMinutes()+minsDifference);
													db.feeds.update(
														{ _id: curFeed._id }, 
														{ $set: { ischecking: 0, nextrun: curDate, thereisanissue: 0 } },
														function(err, updated) {
															if( err || !updated ){
																console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
																//processFeed();
															} else {
																console.log("Feed is now released:" + curFeed._id);
																//processFeed();
															}
														}
													);
												}
												
											});
										}
										
									});
								}
							});
						});
					} else {
						console.log("We have the feed but the URL is malformed. We will now try to get the correct URL");
						console.log(curFeed.url);
						console.log(curFeed.site);
						if(!validator.isURL(curFeed.site)){
							console.log("Even the site is not correct on this feed. This feed will now be reported to manual check");
							curDate.setHours(curDate.getHours()+48);
							db.feeds.update(
								{ _id: curFeed._id }, 
								{ $set: { ischecking: 0, nextrun: curDate, thereisanissue:1 } },
								function(err, updated) {
									if( err || !updated ){
										console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
										//processFeed();
									} else {
										console.log("Feed is now released:" + curFeed._id);
										//processFeed();
									}
								}
							);
						} else {
							console.log("Site url is valid");
							var curURLcontent = '';
							var siteRequestCallBack = function(error, response, html){
								if(!error && response.statusCode == 200) {
									$ = cheerio.load(html);
									var curFeedLink = '';
									var curSiteLink = curFeed.site;
									if( curSiteLink.slice(-1) == '/'){
										curSiteLink = curSiteLink.substring(0,curSiteLink.length-1);
									}
									$("a").each(function(){
										var feedlink = $(this).attr("href");
										if(!feedlink) feedlink = '';
										if(!validator.isURL(feedlink) && feedlink){
											if(feedlink.substring(0,1) != '/'){
												feedlink = '/'+feedlink;
											}
											feedlink = curSiteLink + feedlink;
										}
										if(feedlink.search("atom") > -1){
											curFeedLink = feedlink;
										}
										
										if(feedlink.search("rss") > -1){
											curFeedLink = feedlink;
										}
									});
									var isFoundTheCurLink = false;
									$("link").each(function() {
										var feedlink = $(this);
										var feedtext = feedlink.text();
										var feedhref = feedlink.attr("href");
										var feedtype = feedlink.attr("type");
										
										if(!isFoundTheCurLink && feedtype == "application/rss+xml"){
											curFeedLink = feedhref;
											if(!validator.isURL(curFeedLink)){
												curFeedLink = curSiteLink + curFeedLink;
											}
											if(validator.isURL(curFeedLink)){
												isFoundTheCurLink = true;
											} else {
												curFeedLink = '';
											}
										}
										if(!isFoundTheCurLink && feedtype == "application/atom+xml"){
											curFeedLink = feedhref;
											if(!validator.isURL(curFeedLink)){
												curFeedLink = curSiteLink + curFeedLink;
											}
											if(validator.isURL(curFeedLink)){
												isFoundTheCurLink = true;
											} else {
												curFeedLink = '';
											}
										}
									});
									
									if(curFeedLink !== ''){
										console.log("We found the feed link");
										db.feeds.update(
											{ _id: curFeed._id }, 
											{ $set: { ischecking: 0, nextrun: curDate, url:curFeedLink } },
											function(err, updated) {
												if( err || !updated ){
													console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
													//processFeed();
												} else {
													console.log("Feed is now released:" + curFeed._id);
													//processFeed();
												}
											}
										);
									} else {
										console.log("We couldn't find the feed link");
										curDate.setHours(curDate.getHours()+48);
										db.feeds.update(
											{ _id: curFeed._id }, 
											{ $set: { ischecking: 0, nextrun: curDate, thereisanissue:1 } },
											function(err, updated) {
												if( err || !updated ){
													console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
													//processFeed();
												} else {
													console.log("Feed is now released:" + curFeed._id);
													//processFeed();
												}
											}
										);
									}
								} else {
									curDate.setHours(curDate.getHours()+48);
									db.feeds.update(
										{ _id: curFeed._id }, 
										{ $set: { ischecking: 0, nextrun: curDate, thereisanissue:1, issuedescription:'Even site link is not working' } },
										function(err, updated) {
											if( err || !updated ){
												console.log("We couldn't release the feed, this is a big issue actually:" + curFeed._id);
												//processFeed();
											} else {
												console.log("Feed is now released:" + curFeed._id);
												//processFeed();
											}
										}
									);
								}
							};
							var siteRequestOptions = {
								uri: curFeed.site,
								timeout: 60000,
								followAllRedirects: true
							};
							request(siteRequestOptions, siteRequestCallBack);
						}
					}
				} else {
					console.log("There is no feed to check");
				}
			}
		}
	);
}



function processFeedOld(){
	var myDate = new Date();
	db.feeds.findOne({_id: mongojs.ObjectId("5483f8a2d3479736550000b0"), url: {$not: /^http.*/}, nextrun: {$lt: myDate}},function(err, data){
		if(err){
			console.log("Error");
		} else {
			if(data){
				
				
				fparser.on('error', function(err) {
					console.log(':::'+err+':::');
					if(err == 'Error: Not a feed'){
						console.log('URL is not resolving to a feed');
						
						var curURLcontent = '';
						
						
						requestCallBack = function(response){
							response.on('data', function(responsechunk){curURLcontent += responsechunk});
							response.on('end',function(){
								//console.log(curURLcontent);
								$ = cheerio.load(curURLcontent);
								var curFeedLink = '';
								$("link").each(function() {
									var feedlink = $(this);
									var feedtext = feedlink.text();
									var feedhref = feedlink.attr("href");
									var feedtype = feedlink.attr("type");
									
									if(feedtype == "application/atom+xml"){
										curFeedLink = feedhref;
									}
									if(feedtype == "application/rss+xml"){
										curFeedLink = feedhref;
									}
								});
								
								if(curFeedLink !== ''){
									console.log("We found the feed link");
									console.log(curFeedLink);
								} else {
									console.log("We couldn't find the feed link");
								}
							});
						};
						var request = http.get(cururl, requestCallBack).end();
						
					}
				});
				fparser.on('end', done);
				fparser.on('meta', function(meta) {
					//console.log(meta);
				});
				fparser.on('readable', function() {
					var post;
					
					while (post = this.read()) {
						//console.log(post);
						//console.log("___________");
						//console.log(post.date);
						//console.log(post.pubdate);
						//console.log(post.pubDate);
						
					}
				});
	        	
	        	if(data.nextrun.getTime() < myDate.getTime()){
	        		//console.log(data.lastdate.getTime() + ' is smaller than now ('+ myDate.getTime() +') Let\'s process');
	        		//console.log(myDate.getTime());
	        		//console.log(myDate);
	        		myDate.setHours(myDate.getHours()-1);
	        		
	        		db.feeds.update({
						_id: mongojs.ObjectId(data._id)
					}, {
						$set: { nextrun: myDate },
					}, {}, function(err, data) {
						if(err){
							console.log("Error: Date is not updated");
						} else {
							console.log("Date is now updated");
						}
					});
	        		
	        		
	        		//console.log("Let's process");
	        	} else {
	        		console.log("Skip this one");
	        	}
	        //	setTimeout(processFeed,10000);
        	} else {
        		setTimeout(processFeed,10000);
        	}
	        	
        	//console.log(data.lastdate);
            //console.log(data.lastdate.getYear());
            //console.log(data.lastdate.getMonth());
            //console.log(data.lastdate.getDay());
            //console.log(data.lastdate.getTime());
            
            //console.log(myDate);
            //console.log(myDate.getYear());
            //console.log(myDate.getMonth());
            //console.log(myDate.getDay());
            //console.log(myDate.getTime());
        }
	});
	//console.log("Hedere" + (process.pid) + ':' + ( ((process.pid%10 +1) * 1000) ));
}

function done(err) {
	if (err) {
		console.log(err);
		console.log(err.stack);
	}
}

function getParams(str) {
	var params = str.split(';').reduce(function (params, param) {
		var parts = param.split('=').map(function (part) { return part.trim(); });
		if (parts.length === 2) {
			params[parts[0]] = parts[1];
		}
		return params;
	}, {});
	return params;
}

function maybeTranslate (res, charset) {
	var iconv;
	// Use iconv if its not utf8 already.
	if (!iconv && charset && !/utf-*8/i.test(charset)) {
		try {
			iconv = new Iconv(charset, 'utf-8');
			console.log('Converting from charset %s to utf-8', charset);
			iconv.on('error', done);
			// If we're using iconv, stream will be the output of iconv
			// otherwise it will remain the output of request
			res = res.pipe(iconv);
		} catch(err) {
			res.emit('error', err);
		}
	}
	return res;
}